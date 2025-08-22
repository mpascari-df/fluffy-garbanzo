from datetime import datetime

# --- Helper Classes and Functions ---

class Literal:
    """Represents a literal value in a mapping rule to distinguish it from a source field path."""
    def __init__(self, value):
        self.value = value

def to_timestamp(date_str):
    """Safely converts an ISO 8601 string to a datetime object."""
    if not date_str:
        return None
    try:
        # Handle 'Z' for UTC timezone representation
        if isinstance(date_str, str) and date_str.endswith('Z'):
            date_str = date_str[:-1] + '+00:00'
        return datetime.fromisoformat(date_str)
    except (ValueError, TypeError):
        # Return None if the date string is malformed or not a string
        return None

def to_int(val):
    """Safely converts a value to an integer."""
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None

def to_float(val):
    """Safely converts a value to a float."""
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None

# --- Mapping Definitions ---

CUSTOMERS_MAPPING = {
    # Target Field: (Source Field or Literal/Lambda, Transformation Function)

    # Primary Keys & Metadata
    'pk_client': ('_id', None),
    'pk_yearmonth': (lambda: datetime.now().strftime("%Y%m"), None),
    'des_data_origin': (Literal('customers'), None),

    # Timestamps
    'ts_customer_created_at': ('createdAt', to_timestamp),
    'ts_updated_at': ('updatedAt', to_timestamp),
    'ts_lead_created_at': ('trial.leadData.createdAt', to_timestamp),
    'ts_lead_updated_at': ('trial.leadData.updatedAt', to_timestamp),
    'ts_last_session': ('lastSessionAt', to_timestamp),
    'ts_first_mixed_plan': ('subscription.firstMixedPlanAt', to_timestamp),

    # Customer Information
    'des_email': ('email', None),
    'des_phone': ('phone', None),
    'des_given_name': ('givenName', None),
    'des_family_name': ('familyName', None),
    'des_country': ('country', None),
    'txt_adress': ('address.line1', None),
    'txt_adress_aux': ('address.line2', None),
    'des_locality': ('address.locality', None),
    'cod_zip': ('address.zip', None),
    'des_country_address': ('address.country', None),

    # Subscription & Financial
    'des_subscription_status': ('subscription.status', None),
    'imp_subscription': ('subscription.amount', to_float),
    'cod_coupon_applied': ('subscription.coupon.applied', None),
    'cod_coupon': ('subscription.coupon.code', None),
    'val_referral_coupon': ('subscription.coupon.referralCount', to_int),
    'val_discount_extra_coupon': ('subscription.coupon.discountPercent', to_float),
    'fk_stripe': ('subscription.stripeCustId', None),
    'imp_subscription_discount': (Literal(None), None),
}

MAPPINGS = {
    'customers': CUSTOMERS_MAPPING,
}