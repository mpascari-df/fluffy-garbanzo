from datetime import datetime

# --- Helper Classes and Functions ---

# Helper functions for leads and customers
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

def to_string(val):
    """Safely converts a value to string."""
    if val is None:
        return None
    return str(val)

def count_array_items(arr):
    """Count items in an array field"""
    if arr is None or not isinstance(arr, list):
        return 0
    return len(arr)

def extract_dog_names(dogs_array):
    """Extract comma-separated dog names from dogs array"""
    if not dogs_array or not isinstance(dogs_array, list):
        return ""
    names = [dog.get('name', '') for dog in dogs_array if isinstance(dog, dict)]
    return ", ".join(filter(None, names))

def sum_dog_weights(dogs_array):
    """Sum weights from dogs array"""
    if not dogs_array or not isinstance(dogs_array, list):
        return None
    total = 0
    for dog in dogs_array:
        if isinstance(dog, dict) and 'weight' in dog:
            try:
                total += float(dog['weight'])
            except (ValueError, TypeError):
                continue
    return total if total > 0 else None

def get_latest_comment_date(comments_array):
    """Get the latest date from internal comments array"""
    if not comments_array or not isinstance(comments_array, list):
        return None
    latest_date = None
    for comment in comments_array:
        if isinstance(comment, dict) and 'date' in comment:
            try:
                comment_date = to_timestamp(comment['date'])
                if comment_date and (latest_date is None or comment_date > latest_date):
                    latest_date = comment_date
            except:
                continue
    return latest_date

# Helper functions for orders
def extract_bag_totals(content_obj):
    """Extract total bags from content.bagList structure"""
    if not content_obj or not isinstance(content_obj, dict):
        return 0
    
    bag_list = content_obj.get('bagList', {})
    if not isinstance(bag_list, dict):
        return 0
    
    total = 0
    for bag_size, meats in bag_list.items():
        if isinstance(meats, dict):
            total += sum(meats.values())
    return total

def extract_meat_totals(content_obj, meat_type):
    """Extract total bags for specific meat type from content.bagList"""
    if not content_obj or not isinstance(content_obj, dict):
        return 0
    
    bag_list = content_obj.get('bagList', {})
    if not isinstance(bag_list, dict):
        return 0
    
    total = 0
    for bag_size, meats in bag_list.items():
        if isinstance(meats, dict) and meat_type in meats:
            total += meats[meat_type]
    return total

def extract_bag_size_count(content_obj, size):
    """Extract count of bags for specific size from content.bagList"""
    if not content_obj or not isinstance(content_obj, dict):
        return 0
    
    bag_list = content_obj.get('bagList', {})
    if not isinstance(bag_list, dict):
        return 0
    
    size_data = bag_list.get(str(size), {})
    if isinstance(size_data, dict):
        return sum(size_data.values())
    return 0

def extract_handlers_count(package_obj):
    """Count handlers from package.handlers array"""
    if not package_obj or not isinstance(package_obj, dict):
        return 0
    
    handlers = package_obj.get('handlers', [])
    return len(handlers) if isinstance(handlers, list) else 0

# Helper functions for payments
def extract_line_items_count(line_items):
    """Count line items"""
    if not line_items or not isinstance(line_items, list):
        return 0
    return len(line_items)

def extract_total_product_qty(line_items):
    """Sum quantities from line items"""
    if not line_items or not isinstance(line_items, list):
        return 0
    
    total = 0
    for item in line_items:
        if isinstance(item, dict) and 'qty' in item:
            try:
                total += int(item['qty'])
            except (ValueError, TypeError):
                continue
    return total

def extract_total_product_grams(line_items):
    """Sum total grams from line items (qty * unitGrams)"""
    if not line_items or not isinstance(line_items, list):
        return 0
    
    total = 0
    for item in line_items:
        if isinstance(item, dict) and 'qty' in item and 'unitGrams' in item:
            try:
                qty = int(item['qty'])
                unit_grams = int(item['unitGrams'])
                total += qty * unit_grams
            except (ValueError, TypeError):
                continue
    return total

def extract_line_items_total_amount(line_items):
    """Sum total amount from line items (qty * unitAmount)"""
    if not line_items or not isinstance(line_items, list):
        return 0.0
    
    total = 0.0
    for item in line_items:
        if isinstance(item, dict) and 'qty' in item and 'unitAmount' in item:
            try:
                qty = int(item['qty'])
                unit_amount = float(item['unitAmount'])
                total += qty * unit_amount
            except (ValueError, TypeError):
                continue
    return total

def extract_products_list(line_items):
    """Extract comma-separated product names"""
    if not line_items or not isinstance(line_items, list):
        return ""
    
    products = []
    for item in line_items:
        if isinstance(item, dict) and 'product' in item:
            products.append(str(item['product']))
    
    return ", ".join(products)

def extract_linked_order_ids(orders_array):
    """Extract comma-separated order IDs"""
    if not orders_array or not isinstance(orders_array, list):
        return ""
    
    return ", ".join(str(order_id) for order_id in orders_array)

def extract_refunds_count(refunds_array):
    """Count refunds"""
    if not refunds_array or not isinstance(refunds_array, list):
        return 0
    return len(refunds_array)

def extract_total_refund_amount(refunds_array):
    """Sum refund amounts"""
    if not refunds_array or not isinstance(refunds_array, list):
        return 0.0
    
    total = 0.0
    for refund in refunds_array:
        if isinstance(refund, dict) and 'amount' in refund:
            try:
                total += float(refund['amount'])
            except (ValueError, TypeError):
                continue
    return total

def extract_latest_refund_status(refunds_array):
    """Get status of most recent refund"""
    if not refunds_array or not isinstance(refunds_array, list):
        return None
    
    latest_refund = None
    latest_date = None
    
    for refund in refunds_array:
        if isinstance(refund, dict) and 'createdAt' in refund:
            try:
                created_at = to_timestamp(refund['createdAt'])
                if created_at and (latest_date is None or created_at > latest_date):
                    latest_date = created_at
                    latest_refund = refund
            except:
                continue
    
    return latest_refund.get('status') if latest_refund else None

def extract_latest_refund_reason(refunds_array):
    """Get reason category of most recent refund"""
    if not refunds_array or not isinstance(refunds_array, list):
        return None
    
    latest_refund = None
    latest_date = None
    
    for refund in refunds_array:
        if isinstance(refund, dict) and 'createdAt' in refund:
            try:
                created_at = to_timestamp(refund['createdAt'])
                if created_at and (latest_date is None or created_at > latest_date):
                    latest_date = created_at
                    latest_refund = refund
            except:
                continue
    
    if latest_refund and 'reason' in latest_refund and isinstance(latest_refund['reason'], dict):
        return latest_refund['reason'].get('category')
    return None

# Helper functions for deliveries
def has_label_data(label_data):
    """Check if labelData exists and is not empty"""
    return label_data is not None and len(str(label_data)) > 0

def get_label_data_length(label_data):
    """Get length of labelData string"""
    if label_data is None:
        return 0
    return len(str(label_data))


# Helper functions for leads-archive

def extract_sales_status(sales_obj):
    """Extract status from sales object"""
    if not sales_obj or not isinstance(sales_obj, dict):
        return None
    return sales_obj.get('status')

def extract_sales_assigned_at(sales_obj):
    """Extract assignedAt timestamp from sales object"""
    if not sales_obj or not isinstance(sales_obj, dict):
        return None
    return to_timestamp(sales_obj.get('assignedAt'))

def extract_sales_reassignment_count(sales_obj):
    """Extract reassignmentCount from sales object"""
    if not sales_obj or not isinstance(sales_obj, dict):
        return 0
    return to_int(sales_obj.get('reassignmentCount', 0))

def extract_sales_comments_count(sales_obj):
    """Count comments from sales object"""
    if not sales_obj or not isinstance(sales_obj, dict):
        return 0
    comments = sales_obj.get('comments', [])
    return len(comments) if isinstance(comments, list) else 0

# Helper functions for contacts-logs

def extract_logs_count(logs_array):
    """Count number of communication logs"""
    if not logs_array or not isinstance(logs_array, list):
        return 0
    return len(logs_array)

def extract_last_log_type(logs_array):
    """Extract event type of the last log entry"""
    if not logs_array or not isinstance(logs_array, list) or len(logs_array) == 0:
        return None
    
    last_log = logs_array[-1]  # Assuming logs are in chronological order
    if isinstance(last_log, dict) and 'eventType' in last_log:
        return last_log['eventType']
    return None

def extract_last_log_direction(logs_array):
    """Extract direction of the last log entry"""
    if not logs_array or not isinstance(logs_array, list) or len(logs_array) == 0:
        return None
    
    last_log = logs_array[-1]  # Assuming logs are in chronological order
    if isinstance(last_log, dict) and 'direction' in last_log:
        return last_log['direction']
    return None

def extract_last_log_status(logs_array):
    """Extract status of the last log entry"""
    if not logs_array or not isinstance(logs_array, list) or len(logs_array) == 0:
        return None
    
    last_log = logs_array[-1]  # Assuming logs are in chronological order
    if isinstance(last_log, dict) and 'status' in last_log:
        return last_log['status']
    return None

def extract_last_log_agent(logs_array):
    """Extract agent ID of the last log entry"""
    if not logs_array or not isinstance(logs_array, list) or len(logs_array) == 0:
        return None
    
    last_log = logs_array[-1]  # Assuming logs are in chronological order
    if isinstance(last_log, dict) and 'agent' in last_log:
        return last_log['agent']
    return None

def extract_last_log_timestamp(logs_array):
    """Extract timestamp of the last log entry"""
    if not logs_array or not isinstance(logs_array, list) or len(logs_array) == 0:
        return None
    
    last_log = logs_array[-1]  # Assuming logs are in chronological order
    if isinstance(last_log, dict) and 'startedAt' in last_log:
        return to_timestamp(last_log['startedAt'])
    return None

def extract_last_log_duration(logs_array):
    """Extract duration of the last log entry"""
    if not logs_array or not isinstance(logs_array, list) or len(logs_array) == 0:
        return None
    
    last_log = logs_array[-1]  # Assuming logs are in chronological order
    if isinstance(last_log, dict) and 'duration' in last_log:
        return to_int(last_log['duration'])
    return None

def extract_total_duration(logs_array):
    """Sum duration from all log entries"""
    if not logs_array or not isinstance(logs_array, list):
        return 0
    
    total = 0
    for log in logs_array:
        if isinstance(log, dict) and 'duration' in log:
            try:
                total += int(log['duration'])
            except (ValueError, TypeError):
                continue
    return total

def extract_call_count(logs_array):
    """Count number of calls in logs"""
    if not logs_array or not isinstance(logs_array, list):
        return 0
    
    count = 0
    for log in logs_array:
        if isinstance(log, dict) and log.get('eventType') == 'call':
            count += 1
    return count

def extract_email_count(logs_array):
    """Count number of emails in logs"""
    if not logs_array or not isinstance(logs_array, list):
        return 0
    
    count = 0
    for log in logs_array:
        if isinstance(log, dict) and log.get('eventType') == 'email':
            count += 1
    return count

def extract_sms_count(logs_array):
    """Count number of SMS in logs"""
    if not logs_array or not isinstance(logs_array, list):
        return 0
    
    count = 0
    for log in logs_array:
        if isinstance(log, dict) and log.get('eventType') == 'sms':
            count += 1
    return count

# Helper functions for retentions

def extract_contact_channels_count(channels_array):
    """Count number of contact channels"""
    if not channels_array or not isinstance(channels_array, list):
        return 0
    return len(channels_array)

def extract_contact_channels_list(channels_array):
    """Extract comma-separated list of contact channels"""
    if not channels_array or not isinstance(channels_array, list):
        return ""
    return ", ".join(str(channel) for channel in channels_array)

def extract_reason_category(reason_obj):
    """Extract category from reasonForPause object"""
    if not reason_obj or not isinstance(reason_obj, dict):
        return None
    return reason_obj.get('category')

def extract_reason_subcategory(reason_obj):
    """Extract subcategory from reasonForPause object"""
    if not reason_obj or not isinstance(reason_obj, dict):
        return None
    return reason_obj.get('subcategory')

# Helper functions for notifications

def extract_data_field(data_obj):
    """Safely convert data object to string representation if present"""
    if not data_obj or not isinstance(data_obj, dict):
        return None
    try:
        return str(data_obj)  # Basic string representation
    except:
        return None

def is_read(read_at):
    """Determine if notification has been read based on readAt timestamp"""
    return read_at is not None

# --- Mapping Definitions ---

# Leads mapping based on actual MongoDB structure (399 docs analyzed)
LEADS_MAPPING = {
    # Primary Keys & Metadata
    'pk_client': ('_id', to_string),
    'pk_yearmonth': (lambda: datetime.now().strftime("%Y%m"), None),
    'des_data_origin': (Literal('leads'), None),

    # Timestamps - based on actual availability
    'ts_lead_created_at': ('createdAt', to_timestamp),
    'ts_lead_updated_at': ('leadUpdatedAt', to_timestamp),
    'ts_updated_at': ('updatedAt', to_timestamp),
    'ts_trial_delivery_default': ('defaultDeliveryDate', to_timestamp),
    'ts_trial_delivery': ('trialDeliveryDate', to_timestamp),
    'ts_sales_assigned': ('sales.assignedAt', to_timestamp),
    'ts_sales_state_updated': ('sales.stateUpdatedAt', to_timestamp),
    'ts_payment_attempted': ('paymentLog.attemptedAt', to_timestamp),

    # Lead Information
    'des_email': ('email', None),
    'des_phone': ('phone', None),
    'des_given_name': ('givenName', None),
    'des_family_name': ('familyName', None),
    'des_country': ('country', None),
    'cod_zip': ('zip', None),
    'txt_address': ('address.line1', None),
    'txt_address_aux': ('address.line2', None),
    'des_locality': ('address.locality', None),
    'des_country_address': ('address.country', None),

    # Sales Data
    'des_sales_status': ('sales.status', None),
    'fk_sales_agent': ('sales.assignedTo', None),
    'val_assignment_count': ('sales.assignmentCount', to_int),
    'val_usage_count': ('usageCount', to_int),
    'txt_not_interested_reason': ('sales.notInterestedReason.category', None),
    'des_not_interested_subcategory': ('sales.notInterestedReason.subcategory', None),

    # Financial Data
    'imp_trial_amount': ('trialAmount', to_float),
    'imp_trial_discount': ('trialDiscount', to_float),
    'imp_subscription_amount': ('subscriptionAmount', to_float),
    'imp_subscription_discount': ('subscriptionDiscount', to_float),
    'pct_subscription_discount': ('subscriptionDiscountPercent', to_float),
    'val_orders_in_cycle': ('ordersInCycle', to_int),

    # Acquisition
    'des_acquisition_source_first': ('acquisition.first.source', None),
    'des_acquisition_source_last': ('acquisition.last.source', None),

    # Flags & Features
    'flg_mixed_plan': ('isMixedPlan', None),
    'cod_pricing_factor': ('subscription.pricingFactor', None),
    'flg_anonymous': ('isAnonymous', None),
    'flg_contact_by_sms': ('contactBySMS', None),

    # Optional fields
    'cod_coupon': ('coupon', None),
    'cod_campaign_id': ('campaignId', None),
    'txt_payment_log_source': ('paymentLog.source', None),
    
    # Standard
    'des_source': (Literal('mongo'), None),
}

# Customers mapping based on actual MongoDB structure (2634 docs analyzed)
CUSTOMERS_MAPPING = {
    # Primary Keys & Metadata
    'pk_client': ('_id', to_string),
    'pk_yearmonth': (lambda: datetime.now().strftime("%Y%m"), None),
    'des_data_origin': (Literal('customers'), None),

    # Timestamps
    'ts_customer_created_at': ('createdAt', to_timestamp),
    'ts_updated_at': ('updatedAt', to_timestamp),
    'ts_last_session': ('lastSessionAt', to_timestamp),

    # Customer Information
    'des_email': ('email', None),
    'des_phone': ('phone', None),
    'des_given_name': ('givenName', None),
    'des_family_name': ('familyName', None),
    'des_country': ('country', None),
    'txt_address': ('address.line1', None),
    'txt_address_aux': ('address.line2', None),
    'des_locality': ('address.locality', None),
    'cod_zip': ('address.zip', None),
    'des_country_address': ('address.country', None),

    # Trial Data
    'imp_trial_amount': ('trial.amount', to_float),
    'imp_trial_discount': ('trial.discount', to_float),
    'val_trial_total_daily_grams': ('trial.consolidatedTrial.totalDailyGrams', to_int),
    'val_trial_bag_count': ('trial.consolidatedTrial.bagCount', to_int),

    # Subscription Data
    'des_subscription_status': ('subscription.status', None),
    'fk_stripe_customer': ('subscription.stripeCustId', None),
    'cod_card_last4': ('subscription.cardLast4', None),
    'imp_subscription_amount': ('subscription.amount', to_float),
    'val_orders_in_cycle': ('subscription.ordersInCycle', to_int),
    'val_payment_cycle_weeks': ('subscription.paymentCycleWeeks', to_int),
    'val_total_daily_grams': ('subscription.totalDailyGrams', to_int),
    'val_payment_issues_count': ('subscription.paymentIssuesCount', to_int),
    'des_delivery_company': ('subscription.deliveryCompany', None),
    'val_cooling_packs_qty': ('subscription.coolingPacksQty', to_int),
    'pct_computed_discount': ('subscription.computedDiscountPercent', to_float),

    # Coupon Data
    'cod_coupon': ('subscription.coupon.code', None),
    'val_referral_count': ('subscription.coupon.referralCount', to_int),
    'pct_coupon_discount': ('subscription.coupon.discountPercent', to_float),

    # Active Records
    'val_active_orders_count': ('subscription.activeOrders', count_array_items),
    'cod_active_payment': ('subscription.activePayment', None),

    # Flags
    'flg_review_invitation_pending': ('subscription.isReviewInvitationPending', None),
    'des_last_review_invitation': ('subscription.lastReviewInvitation', None),
    'flg_contacted_after_pause': ('subscription.wasContactedAfterPause', None),
    'flg_contacted_after_status_update': ('subscription.isContactedAfterStatusUpdated', None),

    # Integration flags
    'flg_updated_on_hubspot': ('isUpdatedOnHubspot', None),
    'flg_updated_on_iterable': ('isUpdatedOnIterable', None),

    # Legacy and Optional
    'val_legacy_id': ('legacyId', to_int),
    'des_acquisition_form_number': ('acquisition.formNumber', to_string),

    # Dogs data (aggregated)
    'val_dogs_count': ('dogs', count_array_items),
    'txt_dog_names': ('dogs', extract_dog_names),
    'val_total_dog_weight': ('dogs', sum_dog_weights),

    # Comments (aggregated)
    'val_internal_comments_count': ('internalComments', count_array_items),
    'ts_last_internal_comment': ('internalComments', get_latest_comment_date),

    # Standard
    'des_source': (Literal('mongo'), None),
}

# Orders mapping based on actual MongoDB structure (19,238 docs analyzed)
ORDERS_MAPPING = {
    # Primary Keys & Metadata
    'pk_order': ('_id', to_string),
    'pk_yearmonth': (lambda: datetime.now().strftime("%Y%m"), None),
    'des_data_origin': (Literal('orders'), None),

    # Order Identification
    'fk_customer': ('custId', None),
    'cod_payment': ('payment', None),
    'des_full_name': ('fullName', None),
    
    # Timestamps
    'ts_order_created': ('createdAt', to_timestamp),
    'ts_order_updated': ('updatedAt', to_timestamp),
    'ts_delivery_date': ('deliveryDate', to_timestamp),
    'ts_tentative_delivery_date': ('tentativeDeliveryDate', to_timestamp),
    'ts_delivery_issue_date': ('delivery.issueDate', to_timestamp),
    'ts_delivery_issue_added': ('delivery.issueAddedAt', to_timestamp),

    # Order Status & Classification
    'des_order_status': ('status', None),
    'des_country': ('country', None),
    
    # Address Information
    'txt_address_line1': ('address.line1', None),
    'txt_address_line2': ('address.line2', None),
    'des_locality': ('address.locality', None),
    'cod_zip': ('address.zip', None),
    'des_address_country': ('address.country', None),
    
    # Contact Info
    'des_email': ('email', None),
    'des_phone': ('phone', None),

    # Order Characteristics - Boolean Flags
    'flg_is_trial': ('isTrial', None),
    'flg_is_secondary': ('isSecondary', None),
    'flg_is_last_in_cycle': ('isLastInCycle', None),
    'flg_is_for_robots': ('isForRobots', None),
    'flg_is_additional': ('isAdditional', None),
    'flg_is_first_renewal': ('isFirstRenewal', None),
    'flg_is_mixed_plan': ('isMixedPlan', None),
    'flg_is_rescheduled': ('isRescheduled', None),
    'flg_is_express_delivery': ('isExpressDelivery', None),
    'flg_has_christmas_extra': ('hasChristmasExtra', None),
    'flg_has_additional_ice_bags': ('hasAdditionalIceBags', None),
    'flg_is_agency_pickup': ('isAgencyPickup', None),
    'flg_address_is_locked': ('addressIsLocked', None),
    'flg_is_legacy': ('isLegacy', None),
    'flg_updated_while_processing': ('updatedWhileProcessing', None),
    'flg_notification_sent': ('notificationSent', None),

    # Package Details
    'val_cooling_packs_qty': ('coolingPacksQty', to_int),
    'val_package_bag_count': ('package.bagCount', to_int),
    'val_total_package_count': ('package.totalPackageCount', to_int),
    'val_total_weight_kg': ('package.totalWeightKg', to_float),
    'val_package_handlers_count': ('package', extract_handlers_count),
    'flg_package_has_issue': ('package.hasIssue', None),
    'des_package_issue_category': ('package.issueType.category', None),
    'val_delta_days': ('deltaDays', to_int),
    'des_additional_order_reason': ('additionalOrderReason', None),
    'des_locked_by': ('lockedBy', None),
    'val_eligible_promotional_extras_qty': ('eligiblePromotionalExtrasQty', to_int),

    # Delivery Details
    'des_delivery_company': ('delivery.deliveryCompany', None),
    'flg_delivery_has_issue': ('delivery.hasIssue', None),
    'des_delivery_issue_category': ('delivery.issueType.category', None),

    # Content - Aggregated from bagList
    'val_total_bags': ('content', extract_bag_totals),
    'val_chicken_bags': ('content', lambda x: extract_meat_totals(x, 'chicken')),
    'val_salmon_bags': ('content', lambda x: extract_meat_totals(x, 'salmon')),
    'val_beef_bags': ('content', lambda x: extract_meat_totals(x, 'beef')),
    'val_turkey_bags': ('content', lambda x: extract_meat_totals(x, 'turkey')),
    'val_bag_size_100_count': ('content', lambda x: extract_bag_size_count(x, 100)),
    'val_bag_size_300_count': ('content', lambda x: extract_bag_size_count(x, 300)),
    'val_bag_size_400_count': ('content', lambda x: extract_bag_size_count(x, 400)),
    'val_bag_size_500_count': ('content', lambda x: extract_bag_size_count(x, 500)),

    # Standard
    'des_source': (Literal('mongo'), None),
}

# Payments mapping based on actual MongoDB structure (11,215 docs analyzed)
PAYMENTS_MAPPING = {
    # Primary Keys & Metadata
    'pk_payment': ('_id', to_string),
    'pk_yearmonth': (lambda: datetime.now().strftime("%Y%m"), None),
    'des_data_origin': (Literal('payments'), None),

    # Payment Identification & Linking
    'fk_customer': ('custId', None),
    'val_linked_orders_count': ('orders', count_array_items),
    'txt_linked_order_ids': ('orders', extract_linked_order_ids),
    
    # Timestamps
    'ts_payment_date': ('date', to_timestamp),
    'ts_payment_created': ('createdAt', to_timestamp),
    'ts_payment_updated': ('updatedAt', to_timestamp),
    'ts_delivery_date': ('deliveryDate', to_timestamp),

    # Payment Status & Processing
    'des_payment_status': ('status', None),
    'des_country': ('country', None),
    'val_failed_attempts_count': ('failedAttemptsCount', to_int),
    'des_error_code': ('errorCode', None),

    # Financial Amounts
    'imp_payment_amount': ('amount', to_float),
    'imp_invoice_amount': ('invoiceAmount', to_float),
    'imp_discount_amount': ('discount', to_float),
    'pct_discount_percent': ('discountPercent', to_float),
    'imp_extras_amount': ('extrasAmount', to_float),
    'imp_shipping_amount': ('shippingAmount', to_float),
    'imp_additional_delivery_amount': ('additionalDeliveryAmount', to_float),
    'imp_paid_additional_delivery_amount': ('paidAdditionalDeliveryAmount', to_float),
    'imp_delivery_fee': ('deliveryFee', to_float),

    # Stripe Integration
    'fk_stripe_customer': ('stripeCustId', None),
    'cod_stripe_payment_id': ('stripePaymentId', None),
    'cod_stripe_charge_id': ('stripeChargeId', None),
    'cod_stripe_invoice_id': ('stripeInvoiceId', None),

    # Payment Method
    'des_payment_method_type': ('paymentMethodType', None),
    'cod_card_last4': ('cardLast4', None),

    # Discounts Applied
    'pct_subscription_discount_applied': ('discountsApplied.subscriptionDiscountPercent', to_float),
    'val_referral_count_applied': ('discountsApplied.referralCount', to_int),

    # Payment Characteristics - Boolean Flags
    'flg_is_trial': ('isTrial', None),
    'flg_is_first_renewal': ('isFirstRenewal', None),
    'flg_is_rescheduled': ('isRescheduled', None),
    'flg_is_additional': ('isAdditional', None),
    'flg_is_legacy': ('isLegacy', None),
    'flg_renewal_email_sent': ('isRenewalEmailSent', None),

    # Line Items Aggregation
    'val_line_items_count': ('lineItems', extract_line_items_count),
    'val_total_product_qty': ('lineItems', extract_total_product_qty),
    'val_total_product_grams': ('lineItems', extract_total_product_grams),
    'imp_line_items_total_amount': ('lineItems', extract_line_items_total_amount),
    'txt_products_list': ('lineItems', extract_products_list),

    # Refunds
    'val_refunds_count': ('refunds', extract_refunds_count),
    'imp_total_refund_amount': ('refunds', extract_total_refund_amount),
    'des_latest_refund_status': ('refunds', extract_latest_refund_status),
    'des_latest_refund_reason': ('refunds', extract_latest_refund_reason),

    # Optional Fields
    'cod_pricing_factor': ('pricingFactor', None),
    'cod_coupon': ('coupon', None),
    'cod_invoice_code': ('invoiceCode', None),

    # Standard
    'des_source': (Literal('mongo'), None),
}

# Deliveries mapping based on actual MongoDB structure (4,171 docs analyzed)
DELIVERIES_MAPPING = {
    # Primary Keys & Metadata
    'pk_delivery': ('_id', to_string),
    'pk_yearmonth': (lambda: datetime.now().strftime("%Y%m"), None),
    'des_data_origin': (Literal('deliveries'), None),

    # Delivery Identification & Linking
    'fk_customer': ('custId', None),
    'des_full_name': ('fullName', None),
    'cod_parcel_id': ('parcelId', None),
    
    # Timestamps
    'ts_delivery_created': ('createdAt', to_timestamp),
    'ts_delivery_updated': ('updatedAt', to_timestamp),
    'ts_delivery_date_scheduled': ('deliveryDate', to_timestamp),
    'ts_delivery_date_actual': ('date', to_timestamp),

    # Delivery Status & Classification
    'des_delivery_status': ('status', None),
    'des_country': ('country', None),
    'des_delivery_company': ('deliveryCompany', None),
    'des_label_group': ('labelGroup', None),
    
    # Address Information
    'txt_address_line1': ('address.line1', None),
    'txt_address_line2': ('address.line2', None),
    'des_locality': ('address.locality', None),
    'cod_zip': ('address.zip', None),
    'des_address_country': ('address.country', None),

    # Delivery Characteristics - Boolean Flags
    'flg_is_for_robots': ('isForRobots', None),
    'flg_cust_label_printed': ('isPrinted.cust', None),
    'flg_internal_label_printed': ('isPrinted.internal', None),

    # Issue Tracking
    'flg_has_issue': ('issue.hasIssue', None),
    'txt_issue_reason': ('issue.reason', None),

    # Label Data
    'flg_has_label_data': ('labelData', has_label_data),
    'val_label_data_length': ('labelData', get_label_data_length),
    'flg_has_internal_label_data': ('internalLabelData', has_label_data),
    'val_internal_label_data_length': ('internalLabelData', get_label_data_length),

    # Standard
    'des_source': (Literal('mongo'), None),
}

# Coupons mapping based on actual MongoDB structure (391 docs analyzed)
COUPONS_MAPPING = {
    # Primary Keys & Metadata
    'pk_coupon': ('_id', to_string),
    'pk_yearmonth': (lambda: datetime.now().strftime("%Y%m"), None),
    'des_data_origin': (Literal('coupons'), None),

    # Coupon Identification
    'cod_coupon': ('_id', to_string),  # Coupon code is the _id
    
    # Timestamps
    'ts_coupon_created_at': ('createdAt', to_timestamp),
    'ts_updated_at': ('updatedAt', to_timestamp),  # May not exist in all docs

    # Coupon Properties
    'des_coupon_type': ('type', None),  # external, internal, etc.
    'des_country': ('country', None),  # Country restriction if applicable
    
    # Status Flags
    'flg_is_not_applicable': ('isNotApplicable', None),  # Deactivation flag

    # Standard
    'des_source': (Literal('mongo'), None),
}

# Users-metadata mapping based on actual MongoDB structure (3,531 docs analyzed)
USERS_METADATA_MAPPING = {
    # Primary Keys & Metadata
    'pk_user_metadata': ('_id', to_string),
    'pk_yearmonth': (lambda: datetime.now().strftime("%Y%m"), None),
    'des_data_origin': (Literal('users-metadata'), None),

    # User Authentication
    'fk_customer': ('_id', to_string),  # Customer ID reference
    'txt_password_hash': ('password', None),  # Bcrypt hash
    'cod_verification_token': ('verificationToken', None),  # Email verification
    
    # Timestamps
    'ts_auth_created_at': ('createdAt', to_timestamp),
    'ts_auth_updated_at': ('updatedAt', to_timestamp),
    'ts_last_session_at': ('lastSessionAt', to_timestamp),

    # System Fields
    'val_version': ('__v', to_int),  # Mongoose version field

    # Standard
    'des_source': (Literal('mongo'), None),
}

# Leads-archive mapping based on actual MongoDB structure (3,134 docs analyzed)
LEADS_ARCHIVE_MAPPING = {
    # Primary Keys & Metadata
    'pk_lead_archive': ('_id', to_string),
    'pk_yearmonth': (lambda: datetime.now().strftime("%Y%m"), None),
    'des_data_origin': (Literal('leads-archive'), None),

    # Lead Identification
    'fk_lead': ('_id', to_string),
    
    # Timestamps
    'ts_lead_created_at': ('createdAt', to_timestamp),
    'ts_lead_updated_at': ('leadUpdatedAt', to_timestamp),
    'ts_updated_at': ('updatedAt', to_timestamp),
    'ts_trial_delivery_default': ('defaultDeliveryDate', to_timestamp),
    'ts_trial_delivery': ('trialDeliveryDate', to_timestamp),

    # Contact Information
    'des_email': ('email', None),
    'des_phone': ('phone', None),
    'des_given_name': ('givenName', None),
    'des_family_name': ('familyName', None),
    'des_name': ('name', None),
    'des_country': ('country', None),
    'cod_zip': ('zip', None),

    # Financial Data
    'imp_trial_amount': ('trialAmount', to_float),
    'imp_trial_discount': ('trialDiscount', to_float),
    'imp_subscription_amount': ('subscriptionAmount', to_float),
    'imp_subscription_discount': ('subscriptionDiscount', to_float),
    'pct_subscription_discount': ('subscriptionDiscountPercent', to_float),
    'val_orders_in_cycle': ('ordersInCycle', to_int),

    # Marketing & Tracking
    'val_usage_count': ('usageCount', to_int),
    'val_mailing_stage': ('mailingStage', to_int),
    'cod_coupon': ('coupon', None),
    'cod_campaign_id': ('campaignId', None),
    'des_email_deliverability': ('emailDeliverability', None),

    # Sales Data - from sales object
    'des_sales_status': ('sales', extract_sales_status),
    'ts_sales_assigned_at': ('sales', extract_sales_assigned_at),
    'val_sales_reassignment_count': ('sales', extract_sales_reassignment_count),
    'val_sales_comments_count': ('sales', extract_sales_comments_count),

    # Dogs - from dogs array
    'val_dogs_count': ('dogs', count_array_items),
    'txt_dog_names': ('dogs', extract_dog_names),
    'val_total_dog_weight': ('dogs', sum_dog_weights),

    # Flags
    'flg_mixed_plan': ('isMixedPlan', None),
    'flg_anonymous': ('isAnonymous', None),
    'flg_contact_by_sms': ('contactBySMS', None),
    'flg_updated_on_hubspot': ('isUpdatedOnHubspot', None),
    'flg_updated_on_iterable': ('isUpdatedOnIterable', None),
    'flg_trial_info_email_pending': ('isTrialInfoEmailPending', None),
    'flg_has_purchase_intent': ('hasPurchaseIntent', None),
    'flg_has_gift': ('hasGift', None),
    'flg_notification_sent': ('notificationSent', None),
    'flg_unserved_region': ('unservedRegion', None),

    # Complex Fields - presence flags
    'flg_has_acquisition_data': ('acquisition', lambda x: x is not None),
    'flg_has_shared_info': ('sharedInfo', lambda x: x is not None),
    'flg_has_subscription_data': ('subscription', lambda x: x is not None),
    'flg_has_tracking_data': ('tracking', lambda x: x is not None),
    'flg_has_address': ('address', lambda x: x is not None),
    'flg_has_payment_log': ('paymentLog', lambda x: x is not None),
    'flg_has_appointment_request': ('appointmentRequest', lambda x: x is not None),

    # System Fields
    'val_version': ('__v', to_int),

    # Standard
    'des_source': (Literal('mongo'), None),
}

# Contacts-Logs mapping based on actual MongoDB structure (261 docs analyzed)
CONTACTS_LOGS_MAPPING = {
    # Primary Keys & Metadata
    'pk_contact_log': ('_id', to_string),
    'pk_yearmonth': (lambda: datetime.now().strftime("%Y%m"), None),
    'des_data_origin': (Literal('contacts-logs'), None),

    # Timestamps
    'ts_created_at': ('createdAt', to_timestamp),
    'ts_updated_at': ('updatedAt', to_timestamp),
    
    # Log Aggregations
    'val_logs_count': ('logs', extract_logs_count),
    'val_total_call_duration': ('logs', extract_total_duration),
    'val_call_count': ('logs', extract_call_count),
    'val_email_count': ('logs', extract_email_count),
    'val_sms_count': ('logs', extract_sms_count),
    
    # Last Log Details
    'ts_last_contact': ('logs', extract_last_log_timestamp),
    'des_last_contact_type': ('logs', extract_last_log_type),
    'des_last_contact_direction': ('logs', extract_last_log_direction),
    'des_last_contact_status': ('logs', extract_last_log_status),
    'val_last_contact_duration': ('logs', extract_last_log_duration),
    'fk_last_contact_agent': ('logs', extract_last_log_agent),
    
    # Standard
    'des_source': (Literal('mongo'), None),
}

# Retentions mapping based on actual MongoDB structure (189 docs analyzed)
RETENTIONS_MAPPING = {
    # Primary Keys & Metadata
    'pk_retention': ('_id', to_string),
    'pk_yearmonth': (lambda: datetime.now().strftime("%Y%m"), None),
    'des_data_origin': (Literal('retentions'), None),

    # Customer Reference
    'fk_customer': ('cust', to_string),
    
    # Timestamps
    'ts_created_at': ('createdAt', to_timestamp),
    'ts_updated_at': ('updatedAt', to_timestamp),
    'ts_assigned_at': ('assignedAt', to_timestamp),
    'ts_paused_at': ('pausedAt', to_timestamp),
    'ts_reactivated_at': ('reactivatedAt', to_timestamp),
    'ts_contacted_at': ('contactedAt', to_timestamp),
    'ts_appointment_at': ('appointmentAt', to_timestamp),
    
    # Status and Assignment
    'des_retention_status': ('status', None),
    'val_reassignment_count': ('reassignmentCount', to_int),
    
    # Contact Channels
    'val_contact_channels_count': ('contactChannels', extract_contact_channels_count),
    'txt_contact_channels': ('contactChannels', extract_contact_channels_list),
    
    # Pause Reason
    'des_pause_reason_category': ('reasonForPause', extract_reason_category),
    'des_pause_reason_subcategory': ('reasonForPause', extract_reason_subcategory),
    
    # System and Integration
    'fk_sys_user': ('sysUser', None),
    'cod_zendesk_ticket': ('zendeskTicketId', None),
    
    # Boolean Flags
    'flg_reactivated_by_agent': ('isReactivatedByAgent', None),
    'flg_retention_due_to_agent': ('isRetentionDueToAgent', None),
    
    # Standard
    'des_source': (Literal('mongo'), None),
}

# Notifications mapping based on actual MongoDB structure (60 docs analyzed)
NOTIFICATIONS_MAPPING = {
    # Primary Keys & Metadata
    'pk_notification': ('_id', to_string),
    'pk_yearmonth': (lambda: datetime.now().strftime("%Y%m"), None),
    'des_data_origin': (Literal('notifications'), None),

    # Recipient Information
    'fk_recipient': ('recipient', to_string),
    'des_recipient_model': ('recipientModel', None),
    
    # Document Reference
    'fk_document': ('docId', to_string),
    'des_document_model': ('docModel', None),
    
    # Notification Details
    'des_notification_type': ('notificationType', None),
    'txt_data': ('data', extract_data_field),
    
    # Timestamps
    'ts_created_at': ('createdAt', to_timestamp),
    'ts_updated_at': ('updatedAt', to_timestamp),
    'ts_read_at': ('readAt', to_timestamp),
    
    # Flags
    'flg_is_read': ('readAt', is_read),
    
    # Standard
    'des_source': (Literal('mongo'), None),
}

# Appointmnets mapping based on actual MongoDB structure (5 docs analyzed)
APPOINTMENTS_MAPPING = {
    # Primary Keys & Metadata
    'pk_appointment': ('_id', to_string),
    'pk_yearmonth': (lambda: datetime.now().strftime("%Y%m"), None),
    'des_data_origin': (Literal('appointments'), None),

    # References
    'fk_sys_user': ('sysUserId', to_string),
    'fk_lead': ('leadId', to_string),
    
    # Timestamps
    'ts_created_at': ('createdAt', to_timestamp),
    'ts_updated_at': ('updatedAt', to_timestamp),
    'ts_starts_at': ('startsAt', to_timestamp),
    
    # Standard
    'des_source': (Literal('mongo'), None),
}

MAPPINGS = {
    'customers': CUSTOMERS_MAPPING,
    'leads': LEADS_MAPPING,
    'orders': ORDERS_MAPPING,
    'payments': PAYMENTS_MAPPING,
    'deliveries': DELIVERIES_MAPPING,
    'coupons': COUPONS_MAPPING,
    'users-metadata': USERS_METADATA_MAPPING,
    'leads-archive': LEADS_ARCHIVE_MAPPING,
    'contacts-logs': CONTACTS_LOGS_MAPPING,
    'retentions': RETENTIONS_MAPPING,
    'notifications': NOTIFICATIONS_MAPPING,
    'appointments': APPOINTMENTS_MAPPING,

}