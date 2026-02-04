import re


def normalize_cnpj(value: str) -> str:
    """
    Remove caracteres nao numericos de um CNPJ.

    :param value: CNPJ original.
    :return: CNPJ apenas com digitos.
    """
    return re.sub(r"\D", "", value or "")
