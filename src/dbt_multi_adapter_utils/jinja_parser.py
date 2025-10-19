from dataclasses import dataclass
from enum import Enum

from jinja2 import Environment
from jinja2.lexer import Token, get_lexer


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


_SAFE_FUNCTIONS = {"ref", "source", "var", "config", "this", "target", "env_var"}
_CONTROL_FLOW_KEYWORDS = {
    "if",
    "elif",
    "else",
    "endif",
    "for",
    "endfor",
    "block",
    "endblock",
    "macro",
    "endmacro",
    "set",
    "endset",
}

_START_POS = 0


def _is_safe_expression(tokens: list[Token]) -> bool:
    for token in tokens:
        if token.type == "name":
            return token.value in _SAFE_FUNCTIONS
    return False


def _is_control_flow(tokens: list[Token]) -> bool:
    for token in tokens:
        if token.type == "name":
            return token.value in _CONTROL_FLOW_KEYWORDS
    return False


def _lex_template(template_text: str) -> list[Token] | None:
    env = Environment()
    try:
        lexer = get_lexer(env)
        stream = lexer.tokenize(template_text)
        return list(stream)
    except Exception:
        return None


@dataclass
class _ExpressionTokensResult:
    tokens: list[Token]
    next_index: int


def _collect_expression_tokens(token_stream: list[Token], start_index: int) -> _ExpressionTokensResult:
    expr_tokens = []
    i = start_index

    while i < len(token_stream):
        token = token_stream[i]
        expr_tokens.append(token)
        i += 1

        if token.type in ("variable_end", "block_end"):
            break

    return _ExpressionTokensResult(tokens=expr_tokens, next_index=i)


def _classify_expression(tokens: list[Token], *, is_block: bool) -> _JinjaRegionType:
    if is_block:
        return _JinjaRegionType.CONTROL_FLOW if _is_control_flow(tokens) else _JinjaRegionType.UNSAFE

    return _JinjaRegionType.SAFE_EXPRESSION if _is_safe_expression(tokens) else _JinjaRegionType.UNSAFE


def _create_static_region(start: int, content: str) -> _JinjaRegion:
    return _JinjaRegion(
        start=start,
        end=start + len(content),
        region_type=_JinjaRegionType.STATIC,
        content=content,
    )


def _create_expression_region(start: int, tokens: list[Token], region_type: _JinjaRegionType) -> _JinjaRegion:
    content = "".join(token.value for token in tokens)
    return _JinjaRegion(
        start=start,
        end=start + len(content),
        region_type=region_type,
        content=content,
    )


@dataclass
class _StreamProcessingState:
    regions: list[_JinjaRegion]
    pos: int
    index: int


def _process_data_token(state: _StreamProcessingState, token: Token) -> _StreamProcessingState:
    region = _create_static_region(state.pos, token.value)
    return _StreamProcessingState(
        regions=state.regions + [region],
        pos=state.pos + len(token.value),
        index=state.index + 1,
    )


def _process_expression_token(
    state: _StreamProcessingState, token_stream: list[Token], token: Token
) -> _StreamProcessingState:
    is_block = token.type == "block_begin"
    result = _collect_expression_tokens(token_stream, state.index)

    expr_len = sum(len(t.value) for t in result.tokens)
    region_type = _classify_expression(result.tokens, is_block=is_block)
    region = _create_expression_region(state.pos, result.tokens, region_type)

    return _StreamProcessingState(
        regions=state.regions + [region],
        pos=state.pos + expr_len,
        index=result.next_index,
    )


def _process_other_token(state: _StreamProcessingState, token: Token) -> _StreamProcessingState:
    return _StreamProcessingState(
        regions=state.regions,
        pos=state.pos + len(token.value),
        index=state.index + 1,
    )


def _process_token_stream(token_stream: list[Token]) -> list[_JinjaRegion]:
    state = _StreamProcessingState(regions=[], pos=_START_POS, index=_START_POS)

    while state.index < len(token_stream):
        token = token_stream[state.index]

        if token.type == "data":
            state = _process_data_token(state, token)
        elif token.type in ("variable_begin", "block_begin"):
            state = _process_expression_token(state, token_stream, token)
        else:
            state = _process_other_token(state, token)

    return state.regions


def _analyze_jinja_template(template_text: str) -> list[_JinjaRegion]:
    token_stream = _lex_template(template_text)

    if token_stream is None:
        return [
            _JinjaRegion(
                start=_START_POS,
                end=len(template_text),
                region_type=_JinjaRegionType.UNSAFE,
                content=template_text,
            )
        ]

    regions = _process_token_stream(token_stream)

    if not regions:
        return [_create_static_region(_START_POS, template_text)]

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
