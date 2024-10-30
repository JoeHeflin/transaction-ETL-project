from .common_transformation import SourceToStandardTransformer
from ..utils.logger import logger


class CheckingTransformer(SourceToStandardTransformer):
    def __init__(self):
        super().__init__("sourceA_example")


class CreditCardATransformer(SourceToStandardTransformer):
    def __init__(self):
        super().__init__("sourceB_example")

    def assign_category(self, vendor, category=None):
        mapping = {"Food & Drink": "Restaurants", "Shopping": "Other"}
        if category in mapping:
            return mapping[category]
        return super().assign_category(vendor, category)

    def format_currency(self, value, format_str, min_value):
        if value < 0:
            value *= -1
        return super().format_currency(value, format_str, min_value)

    def validate_value(self, value, allowed_values):
        mapping = {"Sale": "Debit", "Payment": "Credit", "Return": "Credit"}
        if value in mapping:
            return mapping[value]
        else:
            logger.info(f"Invalid value for CCATransformer: {value}")
        return super().validate_value(value, allowed_values)
