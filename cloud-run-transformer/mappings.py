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

def to_string(val):
    """Safely converts a value to string."""
    if val is None:
        return None
    return str(val)

def to_bool(val):
    """Safely converts a value to boolean."""
    if val is None:
        return None
    if isinstance(val, bool):
        return val
    return bool(val)

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

def count_extras(content_obj):
    """Count extras from content.extras array"""
    if not content_obj or not isinstance(content_obj, dict):
        return 0
    extras = content_obj.get('extras', [])
    return len(extras) if isinstance(extras, list) else 0

def count_additional_extras(content_obj):
    """Count additional extras from content.additionalExtras array"""
    if not content_obj or not isinstance(content_obj, dict):
        return 0
    extras = content_obj.get('additionalExtras', [])
    return len(extras) if isinstance(extras, list) else 0

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
    
    last_log = logs_array[-1]
    if isinstance(last_log, dict) and 'eventType' in last_log:
        return last_log['eventType']
    return None

def extract_last_log_direction(logs_array):
    """Extract direction of the last log entry"""
    if not logs_array or not isinstance(logs_array, list) or len(logs_array) == 0:
        return None
    
    last_log = logs_array[-1]
    if isinstance(last_log, dict) and 'direction' in last_log:
        return last_log['direction']
    return None

def extract_last_log_status(logs_array):
    """Extract status of the last log entry"""
    if not logs_array or not isinstance(logs_array, list) or len(logs_array) == 0:
        return None
    
    last_log = logs_array[-1]
    if isinstance(last_log, dict) and 'status' in last_log:
        return last_log['status']
    return None

def extract_last_log_agent(logs_array):
    """Extract agent ID of the last log entry"""
    if not logs_array or not isinstance(logs_array, list) or len(logs_array) == 0:
        return None
    
    last_log = logs_array[-1]
    if isinstance(last_log, dict) and 'agent' in last_log:
        return last_log['agent']
    return None

def extract_last_log_timestamp(logs_array):
    """Extract timestamp of the last log entry"""
    if not logs_array or not isinstance(logs_array, list) or len(logs_array) == 0:
        return None
    
    last_log = logs_array[-1]
    if isinstance(last_log, dict) and 'startedAt' in last_log:
        return to_timestamp(last_log['startedAt'])
    return None

def extract_last_log_duration(logs_array):
    """Extract duration of the last log entry"""
    if not logs_array or not isinstance(logs_array, list) or len(logs_array) == 0:
        return None
    
    last_log = logs_array[-1]
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

def extract_comeback_probability(reason_obj):
    """Extract comeback probability from reasonForPause object"""
    if not reason_obj or not isinstance(reason_obj, dict):
        return None
    prob = reason_obj.get('comebackProbab')
    return to_float(prob) if prob is not None else None

# Helper functions for notifications
def is_read(read_at):
    """Determine if notification has been read based on readAt timestamp"""
    return read_at is not None

# Changelogs helpers
def extract_entity_type(entity_id):
    """Extract entity type from ID prefix (ord_, cust_, pay_, etc)"""
    if not entity_id or not isinstance(entity_id, str):
        return "unknown"
    
    # Common prefixes from analysis
    prefixes = ['ord_', 'cust_', 'pay_', 'lead_', 'lds_', 'sub_', 'del_']
    for prefix in prefixes:
        if entity_id.startswith(prefix):
            return prefix.rstrip('_')
    
    return "other"

def count_changes_by_actor(logs_array, actor_name):
    """Count changes made by specific actor"""
    if not logs_array or not isinstance(logs_array, list):
        return 0
    
    count = 0
    for log in logs_array:
        if isinstance(log, dict) and log.get('updatedBy') == actor_name:
            count += 1
    return count

def get_latest_change_info(logs_array):
    """Get info about the most recent change"""
    if not logs_array or not isinstance(logs_array, list) or len(logs_array) == 0:
        return None, None, None
    
    # Sort by createdAt to get latest
    sorted_logs = sorted(logs_array, 
                        key=lambda x: x.get('createdAt', ''), 
                        reverse=True)
    
    if sorted_logs:
        latest = sorted_logs[0]
        return (to_timestamp(latest.get('createdAt')),
                latest.get('updatedBy'),
                latest.get('key'))
    
    return None, None, None

def extract_top_changed_fields(logs_array, top_n=3):
    """Extract the most frequently changed fields"""
    if not logs_array or not isinstance(logs_array, list):
        return ""
    
    field_counts = {}
    for log in logs_array:
        if isinstance(log, dict) and 'key' in log:
            key = log['key']
            field_counts[key] = field_counts.get(key, 0) + 1
    
    if not field_counts:
        return ""
    
    sorted_fields = sorted(field_counts.items(), key=lambda x: x[1], reverse=True)
    top_fields = [field for field, count in sorted_fields[:top_n]]
    return ", ".join(top_fields)

def count_unique_fields(logs_array):
    """Count unique fields that were changed"""
    if not logs_array or not isinstance(logs_array, list):
        return 0
    
    unique_fields = set()
    for log in logs_array:
        if isinstance(log, dict) and 'key' in log:
            unique_fields.add(log['key'])
    
    return len(unique_fields)

# Stats helpers
def extract_stat_type(stat_id):
    """Extract stat type from ID pattern"""
    if not stat_id or not isinstance(stat_id, str):
        return "unknown"
    
    if 'SALES-STATS' in stat_id:
        return 'sales'
    elif 'PICKING-STATS' in stat_id:
        return 'picking'
    elif 'STATS' in stat_id:
        return 'business'
    
    return "unknown"

def extract_agent_from_stat_id(stat_id):
    """Extract agent/handler ID from stat ID"""
    if not stat_id or not isinstance(stat_id, str):
        return None
    
    parts = stat_id.split('-')
    if len(parts) >= 3:
        return parts[2]
    return None

# Sysusers helpers
def join_array_as_string(arr):
    """Join array elements as comma-separated string"""
    if not arr or not isinstance(arr, list):
        return ""
    return ", ".join(str(item) for item in arr)

def check_nested_field_exists(obj, field_path):
    """Check if a nested field exists"""
    if not obj or not isinstance(obj, dict):
        return False
    
    keys = field_path.split('.')
    current = obj
    
    for key in keys:
        if isinstance(current, dict) and key in current:
            current = current[key]
        else:
            return False
    
    return True

def check_has_acquisition(acquisition_obj):
    """Check if acquisition object exists and has data"""
    return acquisition_obj is not None and isinstance(acquisition_obj, dict)

# --- Mapping Definitions ---

# Customers mapping based on PRODUCTION data (363,806 docs)
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
    'fk_trial_sales_agent': ('trial.salesAgent', None),
    'des_trial_source': ('trial.source', None),

    # Subscription Data - enhanced based on production
    'des_subscription_status': ('subscription.status', None),
    'ts_subscription_status_updated': ('subscription.statusUpdatedAt', to_timestamp),
    'fk_subscription_status_updated_by': ('subscription.statusUpdatedBy', None),
    'fk_stripe_customer': ('subscription.stripeCustId', None),
    'cod_card_last4': ('subscription.cardLast4', None),
    'imp_subscription_amount': ('subscription.amount', to_float),
    'imp_extras_amount': ('subscription.extrasAmount', to_float),
    'val_orders_in_cycle': ('subscription.ordersInCycle', to_int),
    'val_payment_cycle_weeks': ('subscription.paymentCycleWeeks', to_int),
    'val_total_daily_grams': ('subscription.totalDailyGrams', to_int),
    'val_payment_issues_count': ('subscription.paymentIssuesCount', to_int),
    'des_delivery_company': ('subscription.deliveryCompany', None),
    'val_cooling_packs_qty': ('subscription.coolingPacksQty', to_int),
    'pct_computed_discount': ('subscription.computedDiscountPercent', to_float),
    'des_payment_method_id': ('subscription.paymentMethodId', None),
    'des_payment_method_type': ('subscription.paymentMethodType', None),
    'val_paid_orders_count': ('subscription.paidOrders.count', to_int),
    'imp_paid_orders_total': ('subscription.paidOrders.totalAmount', to_float),
    'flg_is_mixed_plan': ('subscription.isMixedPlan', to_bool),
    'ts_first_mixed_plan': ('subscription.firstMixedPlanAt', to_timestamp),
    'ts_subscription_paused': ('subscription.pausedAt', to_timestamp),

    # Pause Reason
    'des_pause_reason_category': ('subscription.reasonForPause.category', None),
    'des_pause_reason_subcategory': ('subscription.reasonForPause.subcategory', None),
    'val_paused_count': ('subscription.pausedCount', to_int),

    # Coupon Data
    'cod_coupon': ('subscription.coupon.code', None),
    'val_referral_count': ('subscription.coupon.referralCount', to_int),
    'pct_coupon_discount': ('subscription.coupon.discountPercent', to_float),

    # Active Records
    'val_active_orders_count': ('subscription.activeOrders', count_array_items),
    'cod_active_payment': ('subscription.activePayment', None),
    'cod_new_cycle_after_order': ('subscription.newCycleAfterOrder', None),

    # Flags
    'flg_review_invitation_pending': ('subscription.isReviewInvitationPending', to_bool),
    'des_last_review_invitation': ('subscription.lastReviewInvitation', None),
    'flg_contacted_after_status_update': ('subscription.isContactedAfterStatusUpdated', to_bool),

    # Integration flags
    'flg_updated_on_hubspot': ('isUpdatedOnHubspot', to_bool),
    'flg_updated_on_iterable': ('isUpdatedOnIterable', to_bool),

    # Legacy and Optional
    'val_legacy_id': ('legacyId', to_int),

    # Acquisition
    'flg_has_acquisition_data': ('acquisition', check_has_acquisition),

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

# Leads mapping based on PRODUCTION data (227,726 docs)
LEADS_MAPPING = {
    # Primary Keys & Metadata
    'pk_client': ('_id', to_string),
    'pk_yearmonth': (lambda: datetime.now().strftime("%Y%m"), None),
    'des_data_origin': (Literal('leads'), None),

    # Timestamps
    'ts_lead_created_at': ('createdAt', to_timestamp),
    'ts_lead_updated_at': ('leadUpdatedAt', to_timestamp),
    'ts_updated_at': ('updatedAt', to_timestamp),
    'ts_trial_delivery_default': ('defaultDeliveryDate', to_timestamp),
    'ts_trial_delivery': ('trialDeliveryDate', to_timestamp),
    'ts_sales_assigned': ('sales.assignedAt', to_timestamp),
    'ts_sales_state_updated': ('sales.stateUpdatedAt', to_timestamp),

    # Lead Information - corrected coverage
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

    # Subscription data
    'fk_stripe_customer_lead': ('subscription.stripeCustId', None),
    'cod_pricing_factor': ('subscription.pricingFactor', None),
    'ts_initial_payment_attempted': ('subscription.initialPaymentAttemptedAt', to_timestamp),
    'cod_stripe_payment_id': ('subscription.stripePaymentId', None),

    # Flags & Features
    'flg_mixed_plan': ('isMixedPlan', to_bool),
    'flg_anonymous': ('isAnonymous', to_bool),
    'flg_contact_by_sms': ('contactBySMS', to_bool),
    'flg_updated_on_iterable': ('isUpdatedOnIterable', to_bool),
    'flg_updated_on_hubspot': ('isUpdatedOnHubspot', to_bool),

    # Optional fields
    'cod_coupon': ('coupon', None),
    'cod_campaign_id': ('campaignId', None),

    # Dogs aggregated
    'val_dogs_count': ('dogs', count_array_items),

    # Payment log
    'flg_has_payment_log': ('paymentLog', lambda x: x is not None),
    
    # Standard
    'des_source': (Literal('mongo'), None),
}

# Orders mapping based on PRODUCTION data (2,904,170 docs)
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

    # Order Characteristics - corrected percentages
    'flg_is_trial': ('isTrial', to_bool),
    'flg_is_secondary': ('isSecondary', to_bool),
    'flg_is_last_in_cycle': ('isLastInCycle', to_bool),
    'flg_is_for_robots': ('isForRobots', to_bool),
    'flg_is_additional': ('isAdditional', to_bool),
    'flg_is_first_renewal': ('isFirstRenewal', to_bool),
    'flg_is_mixed_plan': ('isMixedPlan', to_bool),
    'flg_is_rescheduled': ('isRescheduled', to_bool),
    'flg_is_express_delivery': ('isExpressDelivery', to_bool),
    'flg_has_additional_ice_bags': ('hasAdditionalIceBags', to_bool),
    'flg_is_agency_pickup': ('isAgencyPickup', to_bool),
    'flg_address_is_locked': ('addressIsLocked', to_bool),
    'flg_notification_sent': ('notificationSent', to_bool),

    # Package Details
    'val_cooling_packs_qty': ('coolingPacksQty', to_int),
    'val_package_bag_count': ('package.bagCount', to_int),
    'val_total_package_count': ('package.totalPackageCount', to_int),
    'val_total_weight_kg': ('package.totalWeightKg', to_float),
    'val_package_handlers_count': ('package', extract_handlers_count),
    'flg_package_has_issue': ('package.hasIssue', to_bool),
    'des_package_issue_category': ('package.issueType', None),
    'val_delta_days': ('deltaDays', to_int),
    'des_additional_order_reason': ('additionalOrderReason', None),
    'des_locked_by': ('lockedBy', None),

    # Delivery Details
    'des_delivery_company': ('delivery.deliveryCompany', None),
    'flg_delivery_has_issue': ('delivery.hasIssue', to_bool),
    'des_delivery_issue_category': ('delivery.issueType', None),
    'cod_tracking_url': ('delivery.trackingUrl', None),
    'cod_parcel_id': ('delivery.parcelId', None),
    'des_label_group': ('delivery.labelGroup', None),

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

    # Extras
    'val_extras_count': ('content', count_extras),
    'val_additional_extras_count': ('content', count_additional_extras),

    # Standard
    'des_source': (Literal('mongo'), None),
}

# Payments mapping based on PRODUCTION data (2,106,258 docs)
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

    # Stripe Integration
    'fk_stripe_customer': ('stripeCustId', None),
    'cod_stripe_payment_id': ('stripePaymentId', None),
    'cod_stripe_charge_id': ('stripeChargeId', None),

    # Payment Method
    'des_payment_method_type': ('paymentMethodType', None),
    'cod_card_last4': ('cardLast4', None),

    # Discounts Applied
    'pct_subscription_discount_applied': ('discountsApplied.subscriptionDiscountPercent', to_float),
    'val_referral_count_applied': ('discountsApplied.referralCount', to_int),
    'cod_applied_coupon': ('discountsApplied.appliedCoupon', None),
    'pct_trial_discount_applied': ('discountsApplied.trialDiscountPercent', to_float),

    # Payment Characteristics - Boolean Flags
    'flg_is_trial': ('isTrial', to_bool),
    'flg_is_first_renewal': ('isFirstRenewal', to_bool),
    'flg_is_rescheduled': ('isRescheduled', to_bool),
    'flg_is_additional': ('isAdditional', to_bool),
    'flg_is_legacy': ('isLegacy', to_bool),
    'flg_renewal_email_sent': ('isRenewalEmailSent', to_bool),

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

    # Standard
    'des_source': (Literal('mongo'), None),
}

# Deliveries mapping based on PRODUCTION data (413,010 docs)
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
    'ts_issue_solved_at': ('issue.solvedAt', to_timestamp),

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
    'flg_is_for_robots': ('isForRobots', to_bool),
    'flg_cust_label_printed': ('isPrinted.cust', to_bool),
    'flg_internal_label_printed': ('isPrinted.internal', to_bool),

    # Issue Tracking
    'flg_has_issue': ('issue.hasIssue', to_bool),
    'txt_issue_reason': ('issue.reason', None),

    # Label Data
    'flg_has_label_data': ('labelData', has_label_data),
    'val_label_data_length': ('labelData', get_label_data_length),
    'flg_has_internal_label_data': ('internalLabelData', has_label_data),
    'val_internal_label_data_length': ('internalLabelData', get_label_data_length),

    # Standard
    'des_source': (Literal('mongo'), None),
}

# Coupons mapping
COUPONS_MAPPING = {
    'pk_coupon': ('_id', to_string),
    'pk_yearmonth': (lambda: datetime.now().strftime("%Y%m"), None),
    'des_data_origin': (Literal('coupons'), None),
    'cod_coupon': ('_id', to_string),
    'ts_coupon_created_at': ('createdAt', to_timestamp),
    'des_coupon_type': ('type', None),
    'des_country': ('country', None),
    'flg_is_not_applicable': ('isNotApplicable', to_bool),
    'des_source': (Literal('mongo'), None),
}

# Users-metadata mapping
USERS_METADATA_MAPPING = {
    'pk_user_metadata': ('_id', to_string),
    'pk_yearmonth': (lambda: datetime.now().strftime("%Y%m"), None),
    'des_data_origin': (Literal('users_metadata'), None),
    'fk_customer': ('_id', to_string),
    'txt_password_hash': ('password', None),
    'cod_verification_token': ('verificationToken', None),
    'ts_auth_created_at': ('createdAt', to_timestamp),
    'ts_auth_updated_at': ('updatedAt', to_timestamp),
    'val_version': ('__v', to_int),
    'flg_is_suspended': ('isSuspended', to_bool),
    'des_source': (Literal('mongo'), None),
}

# Leads-archive mapping
LEADS_ARCHIVE_MAPPING = {
    'pk_lead_archive': ('_id', to_string),
    'pk_yearmonth': (lambda: datetime.now().strftime("%Y%m"), None),
    'des_data_origin': (Literal('leads_archive'), None),
    'fk_lead': ('_id', to_string),
    'ts_lead_created_at': ('createdAt', to_timestamp),
    'ts_lead_updated_at': ('leadUpdatedAt', to_timestamp),
    'ts_updated_at': ('updatedAt', to_timestamp),
    'ts_trial_delivery_default': ('defaultDeliveryDate', to_timestamp),
    'ts_trial_delivery': ('trialDeliveryDate', to_timestamp),
    'des_email': ('email', None),
    'des_phone': ('phone', None),
    'des_given_name': ('givenName', None),
    'des_family_name': ('familyName', None),
    'des_country': ('country', None),
    'cod_zip': ('zip', None),
    'imp_trial_amount': ('trialAmount', to_float),
    'imp_trial_discount': ('trialDiscount', to_float),
    'imp_subscription_amount': ('subscriptionAmount', to_float),
    'imp_subscription_discount': ('subscriptionDiscount', to_float),
    'pct_subscription_discount': ('subscriptionDiscountPercent', to_float),
    'val_orders_in_cycle': ('ordersInCycle', to_int),
    'val_usage_count': ('usageCount', to_int),
    'val_mailing_stage': ('mailingStage', to_int),
    'cod_coupon': ('coupon', None),
    'cod_campaign_id': ('campaignId', None),
    'flg_has_email_deliverability': ('emailDeliverability', lambda x: x is not None),
    'des_sales_status': ('sales', extract_sales_status),
    'ts_sales_assigned_at': ('sales', extract_sales_assigned_at),
    'val_sales_reassignment_count': ('sales', extract_sales_reassignment_count),
    'val_sales_comments_count': ('sales', extract_sales_comments_count),
    'val_dogs_count': ('dogs', count_array_items),
    'txt_dog_names': ('dogs', extract_dog_names),
    'val_total_dog_weight': ('dogs', sum_dog_weights),
    'flg_mixed_plan': ('isMixedPlan', to_bool),
    'flg_anonymous': ('isAnonymous', to_bool),
    'flg_contact_by_sms': ('contactBySMS', to_bool),
    'flg_updated_on_hubspot': ('isUpdatedOnHubspot', to_bool),
    'flg_updated_on_iterable': ('isUpdatedOnIterable', to_bool),
    'flg_has_acquisition_data': ('acquisition', lambda x: x is not None),
    'flg_has_shared_info': ('sharedInfo', lambda x: x is not None),
    'flg_has_subscription_data': ('subscription', lambda x: x is not None),
    'flg_has_address': ('address', lambda x: x is not None),
    'flg_has_payment_log': ('paymentLog', lambda x: x is not None),
    'val_version': ('__v', to_int),
    'des_source': (Literal('mongo'), None),
}

# Contacts-logs mapping
CONTACTS_LOGS_MAPPING = {
    'pk_contact_log': ('_id', to_string),
    'pk_yearmonth': (lambda: datetime.now().strftime("%Y%m"), None),
    'des_data_origin': (Literal('contacts_logs'), None),
    'fk_lead': ('_id', to_string),
    'ts_created_at': ('createdAt', to_timestamp),
    'ts_updated_at': ('updatedAt', to_timestamp),
    'val_logs_count': ('logs', extract_logs_count),
    'val_total_call_duration': ('logs', extract_total_duration),
    'val_call_count': ('logs', extract_call_count),
    'val_email_count': ('logs', extract_email_count),
    'val_sms_count': ('logs', extract_sms_count),
    'ts_last_contact': ('logs', extract_last_log_timestamp),
    'des_last_contact_type': ('logs', extract_last_log_type),
    'des_last_contact_direction': ('logs', extract_last_log_direction),
    'des_last_contact_status': ('logs', extract_last_log_status),
    'val_last_contact_duration': ('logs', extract_last_log_duration),
    'fk_last_contact_agent': ('logs', extract_last_log_agent),
    'des_source': (Literal('mongo'), None),
}

# Retentions mapping
RETENTIONS_MAPPING = {
    'pk_retention': ('_id', to_string),
    'pk_yearmonth': (lambda: datetime.now().strftime("%Y%m"), None),
    'des_data_origin': (Literal('retentions'), None),
    'fk_customer': ('cust', to_string),
    'ts_created_at': ('createdAt', to_timestamp),
    'ts_updated_at': ('updatedAt', to_timestamp),
    'ts_assigned_at': ('assignedAt', to_timestamp),
    'ts_paused_at': ('pausedAt', to_timestamp),
    'ts_reactivated_at': ('reactivatedAt', to_timestamp),
    'ts_contacted_at': ('contactedAt', to_timestamp),
    'ts_appointment_at': ('appointmentAt', to_timestamp),
    'des_retention_status': ('status', None),
    'val_reassignment_count': ('reassignmentCount', to_int),
    'val_contact_channels_count': ('contactChannels', extract_contact_channels_count),
    'txt_contact_channels': ('contactChannels', extract_contact_channels_list),
    'des_pause_reason_category': ('reasonForPause', extract_reason_category),
    'des_pause_reason_subcategory': ('reasonForPause', extract_reason_subcategory),
    'val_comeback_probability': ('reasonForPause', extract_comeback_probability),
    'fk_sys_user': ('sysUser', None),
    'cod_zendesk_ticket': ('zendeskTicketId', None),
    'flg_reactivated_by_agent': ('isReactivatedByAgent', to_bool),
    'flg_retention_due_to_agent': ('isRetentionDueToAgent', to_bool),
    'des_source': (Literal('mongo'), None),
}

# Notifications mapping
NOTIFICATIONS_MAPPING = {
    'pk_notification': ('_id', to_string),
    'pk_yearmonth': (lambda: datetime.now().strftime("%Y%m"), None),
    'des_data_origin': (Literal('notifications'), None),
    'fk_recipient': ('recipient', to_string),
    'des_recipient_model': ('recipientModel', None),
    'fk_document': ('docId', to_string),
    'des_document_model': ('docModel', None),
    'des_notification_type': ('notificationType', None),
    'ts_created_at': ('createdAt', to_timestamp),
    'ts_updated_at': ('updatedAt', to_timestamp),
    'ts_read_at': ('readAt', to_timestamp),
    'flg_is_read': ('readAt', is_read),
    'des_source': (Literal('mongo'), None),
}

# Appointments mapping
APPOINTMENTS_MAPPING = {
    'pk_appointment': ('_id', to_string),
    'pk_yearmonth': (lambda: datetime.now().strftime("%Y%m"), None),
    'des_data_origin': (Literal('appointments'), None),
    'fk_sys_user': ('sysUserId', to_string),
    'fk_lead': ('leadId', to_string),
    'ts_created_at': ('createdAt', to_timestamp),
    'ts_updated_at': ('updatedAt', to_timestamp),
    'ts_starts_at': ('startsAt', to_timestamp),
    'txt_notes': ('notes', None),
    'des_source': (Literal('mongo'), None),
}

# Changelogs mapping
CHANGELOGS_MAPPING = {
    'pk_changelog': ('_id', to_string),
    'pk_yearmonth': (lambda: datetime.now().strftime("%Y%m"), None),
    'des_data_origin': (Literal('changelogs'), None),
    'ts_created_at': ('createdAt', to_timestamp),
    'ts_updated_at': ('updatedAt', to_timestamp),
    'des_entity_type': ('_id', extract_entity_type),
    'fk_entity_id': ('_id', to_string),
    'val_total_changes': ('logs', count_array_items),
    'val_system_changes': ('logs', lambda x: count_changes_by_actor(x, 'SYSTEM')),
    'val_api_changes': ('logs', lambda x: count_changes_by_actor(x, 'apikey01')),
    'val_user_changes': ('logs', lambda x: count_array_items(x) - count_changes_by_actor(x, 'SYSTEM') - count_changes_by_actor(x, 'apikey01')),
    'ts_last_change': ('logs', lambda x: get_latest_change_info(x)[0]),
    'des_last_change_actor': ('logs', lambda x: get_latest_change_info(x)[1]),
    'txt_last_change_key': ('logs', lambda x: get_latest_change_info(x)[2]),
    'txt_top_changed_fields': ('logs', extract_top_changed_fields),
    'val_unique_fields_changed': ('logs', count_unique_fields),
    'des_source': (Literal('mongo'), None),
}

# Orders-archive mapping
ORDERS_ARCHIVE_MAPPING = {
    'pk_order': ('_id', to_string),
    'pk_yearmonth': (lambda: datetime.now().strftime("%Y%m"), None),
    'des_data_origin': (Literal('orders_archive'), None),
    'fk_customer': ('custId', None),
    'cod_payment': ('payment', None),
    'des_full_name': ('fullName', None),
    'ts_order_created': ('createdAt', to_timestamp),
    'ts_order_updated': ('updatedAt', to_timestamp),
    'ts_delivery_date': ('deliveryDate', to_timestamp),
    'ts_initial_date': ('initialDate', to_timestamp),
    'des_order_status': ('status', None),
    'des_country': ('country', None),
    'txt_address_line1': ('address.line1', None),
    'txt_address_line2': ('address.line2', None),
    'des_locality': ('address.locality', None),
    'cod_zip': ('address.zip', None),
    'des_address_country': ('address.country', None),
    'des_email': ('email', None),
    'des_phone': ('phone', None),
    'flg_is_trial': ('isTrial', to_bool),
    'flg_is_secondary': ('isSecondary', to_bool),
    'flg_is_last_in_cycle': ('isLastInCycle', to_bool),
    'flg_is_additional': ('isAdditional', to_bool),
    'flg_is_first_renewal': ('isFirstRenewal', to_bool),
    'flg_is_rescheduled': ('isRescheduled', to_bool),
    'flg_is_express_delivery': ('isExpressDelivery', to_bool),
    'flg_has_additional_ice_bags': ('hasAdditionalIceBags', to_bool),
    'flg_address_is_locked': ('addressIsLocked', to_bool),
    'flg_is_legacy': ('isLegacy', to_bool),
    'flg_was_moved': ('wasMoved', to_bool),
    'flg_was_moved_back': ('wasMovedBack', to_bool),
    'flg_was_reset': ('wasReset', to_bool),
    'flg_notification_sent': ('notificationSent', to_bool),
    'val_package_bag_count': ('package.bagCount', to_int),
    'val_total_package_count': ('package.totalPackageCount', to_int),
    'val_total_weight_kg': ('package.totalWeightKg', to_float),
    'des_delivery_company': ('delivery.deliveryCompany', None),
    'flg_delivery_has_issue': ('delivery.hasIssue', to_bool),
    'val_total_bags': ('content.bagCount', to_int),
    'val_chicken_portions': ('content.menu.chicken', to_int),
    'val_turkey_portions': ('content.menu.turkey', to_int),
    'des_locked_by': ('lockedBy', None),
    'des_updated_by': ('__updatedBy', None),
    'val_delta_days': ('deltaDays', to_int),
    'val_version': ('__v', to_int),
    'des_source': (Literal('mongo'), None),
}

# Payments-archive mapping
PAYMENTS_ARCHIVE_MAPPING = {
    'pk_payment': ('_id', to_string),
    'pk_yearmonth': (lambda: datetime.now().strftime("%Y%m"), None),
    'des_data_origin': (Literal('payments_archive'), None),
    'fk_customer': ('custId', None),
    'val_linked_orders_count': ('orders', count_array_items),
    'txt_linked_order_ids': ('orders', extract_linked_order_ids),
    'ts_payment_date': ('date', to_timestamp),
    'ts_payment_created': ('createdAt', to_timestamp),
    'ts_payment_updated': ('updatedAt', to_timestamp),
    'ts_initial_date': ('initialDate', to_timestamp),
    'des_payment_status': ('status', None),
    'des_country': ('country', None),
    'val_failed_attempts_count': ('failedAttemptsCount', to_int),
    'flg_first_attempt_failed': ('firstAttemptFailed', to_bool),
    'imp_payment_amount': ('amount', to_float),
    'imp_invoice_amount': ('invoiceAmount', to_float),
    'imp_discount_amount': ('discount', to_float),
    'pct_discount_percent': ('discountPercent', to_float),
    'imp_extras_amount': ('extrasAmount', to_float),
    'imp_shipping_amount': ('shippingAmount', to_float),
    'fk_stripe_customer': ('stripeCustId', None),
    'cod_stripe_payment_id': ('stripePaymentId', None),
    'cod_stripe_invoice_id': ('stripeInvoiceId', None),
    'cod_stripe_charge_id': ('stripeChargeId', None),
    'val_line_items_count': ('lineItems', extract_line_items_count),
    'val_total_product_qty': ('lineItems', extract_total_product_qty),
    'val_total_product_grams': ('lineItems', extract_total_product_grams),
    'imp_line_items_total_amount': ('lineItems', extract_line_items_total_amount),
    'flg_is_trial': ('isTrial', to_bool),
    'flg_is_first_renewal': ('isFirstRenewal', to_bool),
    'flg_is_additional': ('isAdditional', to_bool),
    'flg_is_legacy': ('isLegacy', to_bool),
    'flg_was_moved': ('wasMoved', to_bool),
    'cod_coupon': ('coupon', None),
    'cod_invoice_code': ('invoiceCode', None),
    'val_version': ('__v', to_int),
    'des_source': (Literal('mongo'), None),
}

# Packages mapping
PACKAGES_MAPPING = {
    'pk_package': ('_id', to_string),
    'pk_yearmonth': (lambda: datetime.now().strftime("%Y%m"), None),
    'des_data_origin': (Literal('packages'), None),
    'fk_handler': ('handlerId', None),
    'ts_created_at': ('createdAt', to_timestamp),
    'ts_updated_at': ('updatedAt', to_timestamp),
    'ts_used_at': ('usedAt', to_timestamp),
    'val_bag_count': ('bagCount', to_int),
    'val_daily_grams': ('dailyGrams', to_int),
    'flg_is_trial': ('isTrial', to_bool),
    'flg_is_used': ('usedAt', lambda x: x is not None),
    'des_source': (Literal('mongo'), None),
}

# Engagement-histories mapping
ENGAGEMENT_HISTORIES_MAPPING = {
    'pk_engagement': ('_id', to_string),
    'pk_yearmonth': (lambda: datetime.now().strftime("%Y%m"), None),
    'des_data_origin': (Literal('engagement_histories'), None),
    'ts_created_at': ('createdAt', to_timestamp),
    'ts_updated_at': ('updatedAt', to_timestamp),
    'flg_survey_engaged': ('cust.tests.customerDataSurvey.isEngaged', to_bool),
    'des_survey_value': ('cust.tests.customerDataSurvey.value', None),
    'flg_has_daily_grams_recommendations': ('cust.recommendationData.dailyGrams', lambda x: x is not None and len(x) > 0),
    'flg_has_menu_recommendations': ('cust.recommendationData.menus', lambda x: x is not None and len(x) > 0),
    'des_source': (Literal('mongo'), None),
}

# Geocontext mapping
GEOCONTEXT_MAPPING = {
    'pk_geocontext': ('_id', to_string),
    'pk_yearmonth': (lambda: datetime.now().strftime("%Y%m"), None),
    'des_data_origin': (Literal('geocontext'), None),
    'val_ip_range_start': ('fromIp', to_string),
    'val_ip_range_end': ('toIp', to_string),
    'des_country': ('country', None),
    'des_source': (Literal('mongo'), None),
}

# Invalid-phones mapping
INVALID_PHONES_MAPPING = {
    'pk_phone': ('_id', to_string),
    'pk_yearmonth': (lambda: datetime.now().strftime("%Y%m"), None),
    'des_data_origin': (Literal('invalid_phones'), None),
    'des_phone_number': ('_id', to_string),
    'des_country': ('country', None),
    'des_created_by': ('createdBy', None),
    'ts_created_at': ('createdAt', to_timestamp),
    'des_source': (Literal('mongo'), None),
}

# Sysusers mapping
SYSUSERS_MAPPING = {
    'pk_sysuser': ('_id', to_string),
    'pk_yearmonth': (lambda: datetime.now().strftime("%Y%m"), None),
    'des_data_origin': (Literal('sysusers'), None),
    'des_given_name': ('givenName', None),
    'des_family_name': ('familyName', None),
    'des_email': ('email', None),
    'des_country': ('country', None),
    'ts_created_at': ('createdAt', to_timestamp),
    'ts_updated_at': ('updatedAt', to_timestamp),
    'ts_last_session_at': ('lastSessionAt', to_timestamp),
    'txt_roles': ('roles', join_array_as_string),
    'des_role': ('role', None),
    'txt_manages_countries': ('managesCountries', join_array_as_string),
    'flg_is_suspended': ('isSuspended', to_bool),
    'flg_is_sales_available': ('sales.isAvailable', to_bool),
    'flg_has_sales_tracking': ('sales', lambda x: x is not None),
    'flg_has_retentions': ('retentions', lambda x: x is not None),
    'des_source': (Literal('mongo'), None),
}

# Stats mapping
STATS_MAPPING = {
    'pk_stat': ('_id', to_string),
    'pk_yearmonth': (lambda: datetime.now().strftime("%Y%m"), None),
    'des_data_origin': (Literal('stats'), None),
    'ts_created_at': ('createdAt', to_timestamp),
    'ts_updated_at': ('updatedAt', to_timestamp),
    'val_year': ('year', to_int),
    'val_month': ('month', to_int),
    'des_stat_type': ('_id', extract_stat_type),
    'fk_agent_or_handler': ('_id', extract_agent_from_stat_id),
    'des_stat_country': ('country', None),
    'val_total_assigned_leads': ('totals.assignedLeads.total', to_int),
    'val_total_appointments': ('totals.appointments', to_int),
    'val_total_sales': ('totals.sales', to_int),
    'val_total_not_answered': ('totals.notAnswered', to_int),
    'val_total_not_interested': ('totals.notInterested', to_int),
    'val_total_orders': ('totals.orderCount', to_int),
    'val_total_packages': ('totals.packageCount', to_int),
    'pct_conversion_rate': ('conversionRate', to_float),
    'pct_retention_rate': ('retentionRate', to_float),
    'imp_average_sales': ('averageSales', to_float),
    'des_source': (Literal('mongo'), None),
}

# Sysinfo mapping
SYSINFO_MAPPING = {
    'pk_config': ('_id', to_string),
    'pk_yearmonth': (lambda: datetime.now().strftime("%Y%m"), None),
    'des_data_origin': (Literal('sysinfo'), None),
    'ts_updated_at': ('updatedAt', to_timestamp),
    'des_config_type': ('_id', to_string),
    'flg_has_cron_settings': ('cron', lambda x: x is not None),
    'flg_has_email_settings': ('transactionalEmails', lambda x: x is not None),
    'flg_has_sales_settings': ('sales', lambda x: x is not None),
    'flg_has_robot_settings': ('robots', lambda x: x is not None),
    'flg_has_stock_levels': ('levels', lambda x: x is not None),
    'flg_has_agent_lists': ('_id', lambda x: 'AVAILABLE-AGENTS' in str(x) if x else False),
    'val_chicken_stock': ('levels.chicken.300', to_int),
    'val_salmon_stock': ('levels.salmon.300', to_int),
    'val_beef_stock': ('levels.beef.300', to_int),
    'val_turkey_stock': ('levels.turkey.300', to_int),
    'des_source': (Literal('mongo'), None),
}

# MAPPINGS dictionary
MAPPINGS = {
    'customers': CUSTOMERS_MAPPING,
    'leads': LEADS_MAPPING,
    'orders': ORDERS_MAPPING,
    'payments': PAYMENTS_MAPPING,
    'deliveries': DELIVERIES_MAPPING,
    'coupons': COUPONS_MAPPING,
    'users_metadata': USERS_METADATA_MAPPING,
    'leads_archive': LEADS_ARCHIVE_MAPPING,
    'contacts_logs': CONTACTS_LOGS_MAPPING,
    'retentions': RETENTIONS_MAPPING,
    'notifications': NOTIFICATIONS_MAPPING,
    'appointments': APPOINTMENTS_MAPPING,
    'changelogs': CHANGELOGS_MAPPING,
    'orders_archive': ORDERS_ARCHIVE_MAPPING,
    'payments_archive': PAYMENTS_ARCHIVE_MAPPING,
    'packages': PACKAGES_MAPPING,
    'engagement_histories': ENGAGEMENT_HISTORIES_MAPPING,
    'geocontext': GEOCONTEXT_MAPPING,
    'invalid_phones': INVALID_PHONES_MAPPING,
    'sysusers': SYSUSERS_MAPPING,
    'stats': STATS_MAPPING,
    'sysinfo': SYSINFO_MAPPING,
}