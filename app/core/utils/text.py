import re
import unicodedata


def remover_acentos_caracteres_especiais(texto):
    texto_norm = unicodedata.normalize('NFD', texto)
    texto_limpo = re.sub(r'[^\w\s]', '', texto_norm)
    texto_limpo = re.sub(r'\W+', '_', texto_limpo)

    return texto_limpo
