import re


def kebab_to_camel(kebab_str):
    words = kebab_str.split("-")
    return words[0] + "".join(word.capitalize() for word in words[1:])


def get_district_name(input_string):
    match = re.search(r"(?<=, )([^,]+) District,", input_string)
    if match:
        return match.group(1)
    else:
        return None


def kebab_to_title(kebab_case_string):
    return kebab_case_string.replace("-", " ").title()
