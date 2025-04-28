from jinja2 import Template
from config import PROMPT_TEMPLATE

def build_prompt(user_text: str, style: str, palette: str) -> str:
    """Формирование промта для DALL·E на основе текста, стиля и палитры."""
    template = Template(PROMPT_TEMPLATE)
    prompt = template.render(user_text=user_text, style=style, palette=palette)
    return prompt[:800]  # Обрезка до 800 символов