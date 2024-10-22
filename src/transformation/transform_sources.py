from transformation.common_transformation import SourceToStandardTransformer
from utils.logger import logger

class CheckingTransformer(SourceToStandardTransformer):
    def __init__(self, input_file):
        super().__init__('capital_one_checking', input_file) # TODO: remove source to secrets

    def assign_category(self, vendor, category=None):
        return super().assign_category(vendor, category)
    
class CreditCardATransformer(SourceToStandardTransformer):
    def __init__(self, input_file):
        super().__init__('chase_cc', input_file) # TODO: remove source to secrets

    def assign_category(self, vendor, category=None):
        mapping = {
            "Food & Drink": "Restaurants",
            "Shopping": "Other"
        }
        if category in mapping:
            return mapping[category]
        return super().assign_category(vendor, category)
    
    def format_currency(self, value, format_str, min_value, required):
        if value < 0:
            value *= -1
        return super().format_currency(value, format_str, min_value, required)
    
    def validate_value(self, value, allowed_values, required):
        mapping = {
            "Sale": "Debit",
            "Payment": "Credit",
            "Return": "Credit"
        }
        if value in mapping:
            return mapping[value]
        else:
            logger.info(f"Invalid value for CCATransformer: {value}")
        return super().validate_value(value, allowed_values, required)
