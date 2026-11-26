"""分隔符切分共享辅助：把规则条目里的转义展示串还原成实际字符，再按引号/转义约定切分。

规则条目在界面上以转义形式保存（如 "\\t"、"\\\\"），实际解析前必须先还原；
切分统一走标准库 csv.reader，旧逗号规则的引号与转义兼容行为在此保持不变，
服务层与管线层共用同一套切分逻辑，避免两处页面与解析流水线行为漂移。
"""
from __future__ import annotations

import csv
import io
from typing import Any


def unescape_symbol(text: str) -> str:
    """把规则条目里的转义展示串还原成实际分隔/引号/转义字符。

    仅识别 ``\\t``、``\\n``、``\\r``、``\\\\`` 四种历史写法，其余字符原样返回；
    空串视为未配置（csv.reader 侧用严格默认值兜底）。
    """
    if text is None:
        return ""
    out: list[str] = []
    i = 0
    while i < len(text):
        ch = text[i]
        if ch == "\\" and i + 1 < len(text):
            nxt = text[i + 1]
            mapped = {"t": "\t", "n": "\n", "r": "\r", "\\": "\\"}
            if nxt in mapped:
                out.append(mapped[nxt])
                i += 2
                continue
        out.append(ch)
        i += 1
    return "".join(out)


def split_line(line: str, delimiter: str, quotechar: str, escapechar: str) -> list[str]:
    """按给定分隔规则切分一行，沿用历史引号与转义兼容行为。

    引号/转义字符为空时退回 csv.reader 严格默认（双引号 + 双写转义），
    保证老规则在历史数据上的列与值可重现。
    """
    kwargs: dict[str, Any] = {"delimiter": delimiter}
    if quotechar:
        kwargs["quotechar"] = quotechar
    if escapechar:
        kwargs["escapechar"] = escapechar
    reader = csv.reader(io.StringIO(line), **kwargs)
    for row in reader:
        return row
    return []


def split_preview(lines: list[str], delimiter: str, quotechar: str,
                  escapechar: str) -> list[list[str]]:
    """对样本逐行切分，产出可预览的分列结果（空行保留为空列集合外跳过）。"""
    return [
        split_line(line, delimiter, quotechar, escapechar)
        for line in lines if line.strip()
    ]


def rule_split(line: str, rule_row: dict[str, Any]) -> list[str]:
    """按规则条目数据集里的一行（含转义展示串）切分文本。"""
    return split_line(
        line,
        unescape_symbol(str(rule_row.get("分隔符") or ",")),
        unescape_symbol(str(rule_row.get("引号字符") or '"')),
        unescape_symbol(str(rule_row.get("转义字符") or "\\")),
    )
