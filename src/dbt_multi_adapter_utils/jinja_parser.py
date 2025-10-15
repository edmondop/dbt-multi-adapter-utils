import re
from dataclasses import dataclass
from enum import Enum


class _JinjaRegionType(Enum):
    STATIC = "static"
    SAFE_EXPRESSION = "safe_expression"
    CONTROL_FLOW = "control_flow"
    UNSAFE = "unsafe"


@dataclass
class _JinjaRegion:
    start: int
    end: int
    region_type: _JinjaRegionType
    content: str


@dataclass
class SafetyCheckResult:
    can_rewrite: bool
    reason: str


@dataclass
class SafeSqlRegion:
    start: int
    end: int
    masked_sql: str


_SAFE_JINJA_PATTERN = re.compile(
    r"\{\{\s*(ref|source|var|config|this|target|env_var)\s*\([^}]*\)\s*\}\}"
)
_CONTROL_FLOW_PATTERN = re.compile(
    r"\{%\s*(if|elif|else|endif|for|endfor|block|endblock|macro|endmacro|set|endset)\s"
)
_JINJA_EXPRESSION_PATTERN = re.compile(r"\{\{.*?\}\}|\{%.*?%\}", re.DOTALL)


def _analyze_jinja_template(template_text: str) -> list[_JinjaRegion]:
    regions: list[_JinjaRegion] = []
    last_pos = 0

    for match in _JINJA_EXPRESSION_PATTERN.finditer(template_text):
        if last_pos < match.start():
            regions.append(
                _JinjaRegion(
                    start=last_pos,
                    end=match.start(),
                    region_type=_JinjaRegionType.STATIC,
                    content=template_text[last_pos : match.start()],
                )
            )

        jinja_expr = match.group(0)
        if _SAFE_JINJA_PATTERN.match(jinja_expr):
            region_type = _JinjaRegionType.SAFE_EXPRESSION
        elif _CONTROL_FLOW_PATTERN.match(jinja_expr):
            region_type = _JinjaRegionType.CONTROL_FLOW
        else:
            region_type = _JinjaRegionType.UNSAFE

        regions.append(
            _JinjaRegion(
                start=match.start(),
                end=match.end(),
                region_type=region_type,
                content=jinja_expr,
            )
        )

        last_pos = match.end()

    if last_pos < len(template_text):
        regions.append(
            _JinjaRegion(
                start=last_pos,
                end=len(template_text),
                region_type=_JinjaRegionType.STATIC,
                content=template_text[last_pos:],
            )
        )

    if not regions:
        regions.append(
            _JinjaRegion(
                start=0,
                end=len(template_text),
                region_type=_JinjaRegionType.STATIC,
                content=template_text,
            )
        )

    return regions


class JinjaTemplate:
    def __init__(self, template_text: str):
        self.template_text = template_text
        self._regions: list[_JinjaRegion] | None = None

    @property
    def regions(self) -> list[_JinjaRegion]:
        if self._regions is None:
            self._regions = _analyze_jinja_template(self.template_text)
        return self._regions

    def can_safely_rewrite(self) -> SafetyCheckResult:
        # We can handle control flow - masking works fine for parsing
        # Only reject if we have unsafe/complex Jinja expressions
        has_unsafe = any(r.region_type == _JinjaRegionType.UNSAFE for r in self.regions)

        if has_unsafe:
            return SafetyCheckResult(False, "Template contains complex Jinja expressions")

        return SafetyCheckResult(True, "Template is safe to rewrite")

    def extract_safe_sql_regions(self) -> list[SafeSqlRegion]:
        masked_content = ""
        for region in self.regions:
            if region.region_type == _JinjaRegionType.STATIC:
                masked_content += region.content
            elif region.region_type == _JinjaRegionType.SAFE_EXPRESSION:
                masked_content += " __PLACEHOLDER__ "
            elif region.region_type == _JinjaRegionType.CONTROL_FLOW:
                # Mask control flow with placeholder - add comma for SQL validity
                masked_content += " __JINJA__, "
            else:
                # UNSAFE expressions - mask them too
                masked_content += " __JINJA__, "

        if masked_content.strip():
            return [SafeSqlRegion(0, len(self.template_text), masked_content)]

        return []
