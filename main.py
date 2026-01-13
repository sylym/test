#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import hashlib
import logging
import os
import random
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import requests


TBS_URL = "https://tieba.baidu.com/dc/common/tbs"
FOLLOW_URL = "https://tieba.baidu.com/mo/q/newmoindex"
SIGN_URL = "https://c.tieba.baidu.com/c/c/forum/sign"


@dataclass
class Config:
    max_workers: int = 3                 # 并发数（建议 1~4，越大越快也越容易触发频控）
    connect_timeout: float = 3.0
    timeout: float = 10.0

    # 每次请求前的随机抖动（控制“看起来不像瞬间打满”）
    min_delay: float = 1.15
    max_delay: float = 1.35

    # 单吧最大重试次数（遇到频控/网络问题会重试）
    per_forum_retries: int = 3

    # 失败项二阶段重试：等待 N 秒后，用更保守的节奏顺序重试
    stage2_wait: int = 60
    stage2_min_delay: float = 2.6
    stage2_max_delay: float = 3.2
    stage2_retries: int = 2

    # 获取关注列表最大翻页次数（接口是否支持 pn 取决于实际返回；这里做“防重复”保护）
    max_pages: int = 10

    # 退避基数（指数退避）
    backoff_base: float = 0.6

    debug: bool = False


@dataclass(frozen=True)
class ForumResult:
    forum: str
    ok: bool
    code: str
    msg: str
    already: bool = False
    invalid: bool = False


def _env_int(key: str, default: int) -> int:
    v = os.getenv(key)
    if v is None or v == "":
        return default
    try:
        return int(v)
    except ValueError:
        return default


def _env_float(key: str, default: float) -> float:
    v = os.getenv(key)
    if v is None or v == "":
        return default
    try:
        return float(v)
    except ValueError:
        return default


def load_config() -> Config:
    cfg = Config(
        max_workers=max(1, _env_int("MAX_WORKERS", 3)),
        connect_timeout=_env_float("CONNECT_TIMEOUT", 3.0),
        timeout=_env_float("TIMEOUT", 10.0),
        min_delay=_env_float("MIN_DELAY", 0.15),
        max_delay=_env_float("MAX_DELAY", 0.35),
        per_forum_retries=max(1, _env_int("PER_FORUM_RETRIES", 3)),
        stage2_wait=max(0, _env_int("STAGE2_WAIT", 60)),
        stage2_min_delay=_env_float("STAGE2_MIN_DELAY", 0.6),
        stage2_max_delay=_env_float("STAGE2_MAX_DELAY", 1.2),
        stage2_retries=max(1, _env_int("STAGE2_RETRIES", 2)),
        max_pages=max(1, _env_int("MAX_PAGES", 10)),
        backoff_base=_env_float("BACKOFF_BASE", 0.6),
        debug=os.getenv("DEBUG", "0") == "1",
    )

    # 纠正区间
    cfg.min_delay = max(0.0, cfg.min_delay)
    cfg.max_delay = max(cfg.min_delay, cfg.max_delay)
    cfg.stage2_min_delay = max(0.0, cfg.stage2_min_delay)
    cfg.stage2_max_delay = max(cfg.stage2_min_delay, cfg.stage2_max_delay)
    return cfg


class TiebaSigner:
    def __init__(self, bduss: str, cfg: Config) -> None:
        self.bduss = bduss.strip()
        self.cfg = cfg

        self.log = logging.getLogger("tieba")
        self._local = threading.local()

        self._tbs_lock = threading.Lock()
        self._tbs: str = ""
        self._tbs_ts: float = 0.0

        self.user_agent = (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )

    def _session(self) -> requests.Session:
        """每线程一个 Session，既复用连接又避免共享 Session 的线程安全问题。"""
        s = getattr(self._local, "session", None)
        if s is not None:
            return s

        s = requests.Session()
        s.headers.update(
            {
                "User-Agent": self.user_agent,
                "Accept": "application/json, text/plain, */*",
                "Connection": "keep-alive",
            }
        )
        # 用 cookie jar 更安全（避免拼 Cookie header 发生意外格式问题）
        s.cookies.set("BDUSS", self.bduss, domain=".baidu.com")
        self._local.session = s
        return s

    @staticmethod
    def _client_sign(params: Dict[str, str]) -> str:
        """Tieba client 通用签名：按 key 排序拼接 key=value，再加盐，md5。"""
        base = "".join(f"{k}={params[k]}" for k in sorted(params.keys()))
        base += "tiebaclient!!!"
        return hashlib.md5(base.encode("utf-8")).hexdigest()

    @staticmethod
    def _sleep_jitter(min_delay: float, max_delay: float) -> None:
        time.sleep(random.uniform(min_delay, max_delay))

    def _backoff(self, attempt: int, base: Optional[float] = None) -> float:
        b = self.cfg.backoff_base if base is None else base
        return b * (2 ** (attempt - 1)) + random.uniform(0.0, 0.6)

    def _request_json(
            self,
            method: str,
            url: str,
            *,
            params: Optional[Dict[str, Any]] = None,
            data: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        s = self._session()
        try:
            r = s.request(
                method,
                url,
                params=params,
                data=data,
                timeout=(self.cfg.connect_timeout, self.cfg.timeout),
            )
            r.encoding = "utf-8"
            return r.json()
        except (requests.exceptions.RequestException, ValueError) as e:
            self.log.debug("request_json failed: %s %s (%s)", method, url, type(e).__name__)
            return {}

    def _request_json_retry(
            self,
            method: str,
            url: str,
            *,
            params: Optional[Dict[str, Any]] = None,
            data: Optional[Dict[str, Any]] = None,
            attempts: int = 3,
    ) -> Dict[str, Any]:
        for i in range(1, attempts + 1):
            res = self._request_json(method, url, params=params, data=data)
            if res:
                return res
            time.sleep(self._backoff(i))
        return {}

    def refresh_tbs(self, force: bool = False) -> str:
        """刷新 tbs（加锁避免并发下重复刷新）。"""
        with self._tbs_lock:
            now = time.time()
            if not force and self._tbs and (now - self._tbs_ts) < 20:
                return self._tbs

            res = self._request_json_retry("GET", TBS_URL, attempts=3)
            if not res:
                raise RuntimeError("获取 tbs 失败（网络/响应异常）")

            if res.get("is_login") != 1:
                raise RuntimeError("BDUSS 无效或登录状态失效（is_login != 1）")

            tbs = (res.get("tbs") or "").strip()
            if not tbs:
                raise RuntimeError("获取 tbs 失败（tbs 为空）")

            self._tbs = tbs
            self._tbs_ts = now
            return tbs

    def get_follow_list(self) -> Tuple[List[str], List[str]]:
        """获取关注贴吧列表：返回(待签到, 已签到)。带“防重复”的伪分页。"""
        to_sign: List[str] = []
        already: List[str] = []
        seen: set[str] = set()

        for pn in range(1, self.cfg.max_pages + 1):
            res = self._request_json_retry("GET", FOLLOW_URL, params={"pn": pn}, attempts=3)
            data = res.get("data") or {}
            forums = data.get("like_forum") or []

            if not isinstance(forums, list) or not forums:
                if pn == 1:
                    raise RuntimeError("获取关注列表失败/返回空（可能 BDUSS 失效或接口变更）")
                break

            new_in_page = 0
            for item in forums:
                if not isinstance(item, dict):
                    continue
                name = (item.get("forum_name") or "").strip()
                if not name or name in seen:
                    continue
                seen.add(name)
                new_in_page += 1

                is_sign = str(item.get("is_sign", "0"))
                if is_sign == "0":
                    to_sign.append(name)
                else:
                    already.append(name)

            # 若 pn 不被支持，第二页很可能返回同一批数据 => new_in_page == 0
            if new_in_page == 0:
                break

            has_more = data.get("has_more")
            if has_more in (0, "0", False):
                break

        return to_sign, already

    @staticmethod
    def _looks_like_login_expired(msg: str) -> bool:
        m = (msg or "").lower()
        return ("未登录" in msg) or ("请登录" in msg) or ("login" in m and "need" in m)

    @staticmethod
    def _looks_like_tbs_invalid(msg: str) -> bool:
        return "tbs" in (msg or "").lower()

    @staticmethod
    def _looks_like_rate_limited(code: str, msg: str) -> bool:
        return ("频繁" in msg) or ("太快" in msg) or ("稍后" in msg) or (code in {"340006", "340007", "340008", "340009"})

    @staticmethod
    def _looks_like_forum_invalid(msg: str) -> bool:
        m = (msg or "")
        return ("不存在" in m) or ("无此吧" in m) or ("not exist" in m.lower())

    def sign_forum(self, forum: str, *, min_delay: float, max_delay: float, retries: int) -> ForumResult:
        """单个贴吧签到（含错误分类与重试）。"""
        for attempt in range(1, retries + 1):
            self._sleep_jitter(min_delay, max_delay)

            tbs = self._tbs or ""
            if not tbs:
                self.refresh_tbs(force=True)
                tbs = self._tbs

            payload: Dict[str, str] = {"kw": forum, "tbs": tbs}
            payload["sign"] = self._client_sign(payload)

            res = self._request_json("POST", SIGN_URL, data=payload)
            if not res:
                wait = self._backoff(attempt)
                self.log.warning("[%s] 无响应/非JSON，%.1fs 后重试(%d/%d)", forum, wait, attempt, retries)
                time.sleep(wait)
                continue

            code = str(res.get("error_code", ""))
            msg = str(res.get("error_msg", ""))

            # 成功
            if code == "0":
                return ForumResult(forum=forum, ok=True, code=code, msg=msg)

            # 已签也视为成功（提升“成功率”）
            if code == "160002" or ("已签" in msg) or ("already" in msg.lower()):
                return ForumResult(forum=forum, ok=True, code=code, msg=msg, already=True)

            # 登录失效：直接终止（继续重试也没意义）
            if self._looks_like_login_expired(msg):
                raise RuntimeError(f"登录状态失效：{code} {msg}")

            # 吧不存在/无效：不再重试
            if self._looks_like_forum_invalid(msg):
                return ForumResult(forum=forum, ok=False, code=code, msg=msg, invalid=True)

            # tbs 可能失效：刷新后重试
            if self._looks_like_tbs_invalid(msg):
                self.log.info("[%s] tbs 可能失效，刷新后重试(%d/%d)", forum, attempt, retries)
                try:
                    self.refresh_tbs(force=True)
                except Exception as e:
                    self.log.warning("刷新 tbs 失败：%s", type(e).__name__)
                continue

            # 频控：等待更久再试
            if self._looks_like_rate_limited(code, msg):
                wait = self._backoff(attempt, base=max(self.cfg.backoff_base, 2.0))
                self.log.warning("[%s] 可能触发频控：%s(%s)，等待 %.1fs", forum, msg or "rate limited", code, wait)
                time.sleep(wait)
                continue

            # 其他错误：有限重试
            if attempt < retries:
                wait = self._backoff(attempt)
                self.log.warning("[%s] 失败：%s(%s)，%.1fs 后重试(%d/%d)", forum, msg or "unknown", code, wait, attempt, retries)
                time.sleep(wait)
                continue

            return ForumResult(forum=forum, ok=False, code=code, msg=msg)

        return ForumResult(forum=forum, ok=False, code="", msg="exhausted")

    @staticmethod
    def write_step_summary(md: str) -> None:
        path = os.getenv("GITHUB_STEP_SUMMARY")
        if not path:
            return
        try:
            with open(path, "a", encoding="utf-8") as f:
                f.write(md.rstrip() + "\n")
        except Exception:
            pass

    def run(self) -> int:
        self.refresh_tbs(force=True)

        to_sign, already = self.get_follow_list()
        total = len(to_sign) + len(already)
        self.log.info("关注贴吧：%d；已签到：%d；待签到：%d", total, len(already), len(to_sign))

        if not to_sign:
            md = f"## Tieba 签到结果\n\n- 关注：{total}\n- 已签到：{len(already)}\n- 待签到：0（本次无需操作）\n"
            self.write_step_summary(md)
            return 0

        # 第一阶段：适度并发 + 轻抖动
        stage1_results: List[ForumResult] = []
        with ThreadPoolExecutor(max_workers=self.cfg.max_workers) as ex:
            futs = {
                ex.submit(
                    self.sign_forum,
                    name,
                    min_delay=self.cfg.min_delay,
                    max_delay=self.cfg.max_delay,
                    retries=self.cfg.per_forum_retries,
                ): name
                for name in to_sign
            }
            for fut in as_completed(futs):
                name = futs[fut]
                try:
                    r = fut.result()
                    stage1_results.append(r)
                except Exception as e:
                    # 登录失效等致命错误：直接失败退出
                    raise RuntimeError(f"签到线程异常（{name}）：{e}") from e

        ok = {r.forum for r in stage1_results if r.ok}
        invalid = {r.forum for r in stage1_results if r.invalid}
        failed = {r.forum for r in stage1_results if (not r.ok and not r.invalid)}

        # 第二阶段：只对失败项做更保守的顺序重试（提高成功率）
        stage2_results: List[ForumResult] = []
        if failed and self.cfg.stage2_wait > 0:
            self.log.warning("第一阶段失败 %d 个，等待 %ds 后进行保守重试…", len(failed), self.cfg.stage2_wait)
            time.sleep(self.cfg.stage2_wait)
            self.refresh_tbs(force=True)

            for name in sorted(failed):
                r = self.sign_forum(
                    name,
                    min_delay=self.cfg.stage2_min_delay,
                    max_delay=self.cfg.stage2_max_delay,
                    retries=self.cfg.stage2_retries,
                )
                stage2_results.append(r)

            ok |= {r.forum for r in stage2_results if r.ok}
            invalid |= {r.forum for r in stage2_results if r.invalid}
            failed = {name for name in failed if name not in ok and name not in invalid}

        md_lines = [
            "## Tieba 签到结果",
            "",
            f"- 关注：{total}",
            f"- 已签到（接口返回）：{len(already)}",
            f"- 本次成功：{len(ok)}",
            f"- 无效/不存在：{len(invalid)}",
            f"- 失败：{len(failed)}",
        ]
        if failed:
            md_lines += ["", "### 失败列表（截断）", ""]
            show = list(sorted(failed))[:30]
            md_lines += [f"- {x}" for x in show]
            if len(failed) > 30:
                md_lines.append(f"- ...（共 {len(failed)} 个，已截断）")

        md = "\n".join(md_lines) + "\n"
        self.write_step_summary(md)

        # 失败则返回非 0，让 Actions 标红便于关注（你也可以改成永远 0）
        return 1 if failed else 0


def main() -> int:
    cfg = load_config()

    logging.basicConfig(
        level=logging.DEBUG if cfg.debug else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    bduss = os.getenv("BDUSS", "").strip()
    if not bduss:
        print("错误：未找到 BDUSS 环境变量（请在 GitHub Secrets 中配置）。", file=sys.stderr)
        return 2

    app = TiebaSigner(bduss=bduss, cfg=cfg)
    return app.run()


if __name__ == "__main__":
    sys.exit(main())