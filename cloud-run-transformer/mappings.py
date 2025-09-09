from datetime import datetime

# --- Helper Classes and Functions ---

class Literal:
    """Represents a literal value in a mapping rule to distinguish it from a source field path."""
    def __init__(self, value):
        self.value = value

# ============================================================================
# UNIVERSAL ARRAY HANDLER - FIX FOR WIDESPREAD ARRAY ISSUES
# ============================================================================
def safe_extract(value, default=None):
    """
    Universal extractor that safely handles fields that might be:
    - Single values
    - Arrays/lists
    - None/null
    - Empty strings
    
    This addresses the widespread "ambiguous truth value" errors across multiple collections.
    
    Args:
        value: The value to extract (might be single value or array)
        default: Default value to return if extraction fails
        
    Returns:
        The extracted value or default
    """
    if value is None:
        return default
    
    # Handle arrays/lists - take first non-empty value
    if isinstance(value, (list, tuple)):
        for val in value:
            if val is not None and val != '':
                return val
        return default
    
    # Handle empty strings
    if value == '':
        return default
    
    return value

def safe_nested_extract(obj, path, default=None):
    """
    Safely extract nested values that might be arrays at any level.
    
    Args:
        obj: The source object
        path: Dot-separated path (e.g., 'subscription.status')
        default: Default value if extraction fails
        
    Returns:
        The extracted value with array handling at each level
    """
    if not path or not isinstance(obj, dict):
        return default
    
    keys = path.split('.')
    current = obj
    
    for key in keys:
        if isinstance(current, dict) and key in current:
            current = current[key]
            # Apply safe_extract at each level to handle potential arrays
            current = safe_extract(current)
            if current is None:
                return default
        else:
            return default
    
    return current

# ============================================================================
# TYPE CONVERSION WRAPPERS WITH SAFE EXTRACTION
# ============================================================================
def safe_to_string(value):
    """Safely extract and convert to string"""
    extracted = safe_extract(value)
    return to_string(extracted)

def safe_to_int(value):
    """Safely extract and convert to int"""
    extracted = safe_extract(value)
    return to_int(extracted)

def safe_to_float(value):
    """Safely extract and convert to float"""
    extracted = safe_extract(value)
    return to_float(extracted)

def safe_to_bool(value):
    """Safely extract and convert to bool"""
    extracted = safe_extract(value)
    return to_bool(extracted)

def safe_to_timestamp(value):
    """Safely extract and convert to timestamp"""
    extracted = safe_extract(value)
    return to_timestamp(extracted)

# ============================================================================
# SAFE FIELD EXTRACTOR HELPER
# ============================================================================
def safe_field_extractor(path, transform_func=None):
    """
    Create a safe extractor for a field path that handles arrays and nested structures.
    
    Args:
        path: Dot-separated path to the field
        transform_func: Optional transformation function to apply
        
    Returns:
        A function that safely extracts and transforms the field
    """
    def extractor(doc):
        if not isinstance(doc, dict):
            return None
        value = safe_nested_extract(doc, path)
        if transform_func and value is not None:
            return transform_func(value)
        return value
    return extractor

# ============================================================================
# ORIGINAL TYPE CONVERSION HELPERS
# ============================================================================
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

# ============================================================================
# COLLECTION-SPECIFIC HELPER FUNCTIONS
# ============================================================================

# --- Customers/Leads Helpers ---
def count_array_items(arr):
    """Count items in an array field"""
    arr = safe_extract(arr)
    if arr is None or not isinstance(arr, list):
        return 0
    return len(arr)

def extract_dog_names(dogs_array):
    """Extract comma-separated dog names from dogs array"""
    dogs_array = safe_extract(dogs_array)
    if not dogs_array or not isinstance(dogs_array, list):
        return ""
    names = [safe_extract(dog.get('name', '')) for dog in dogs_array if isinstance(dog, dict)]
    return ", ".join(filter(None, names))

def sum_dog_weights(dogs_array):
    """Sum weights from dogs array"""
    dogs_array = safe_extract(dogs_array)
    if not dogs_array or not isinstance(dogs_array, list):
        return None
    total = 0
    for dog in dogs_array:
        if isinstance(dog, dict) and 'weight' in dog:
            try:
                weight = safe_extract(dog['weight'])
                if weight:
                    total += float(weight)
            except (ValueError, TypeError):
                continue
    return total if total > 0 else None

def get_latest_comment_date(comments_array):
    """Get the latest date from internal comments array"""
    comments_array = safe_extract(comments_array)
    if not comments_array or not isinstance(comments_array, list):
        return None
    latest_date = None
    for comment in comments_array:
        if isinstance(comment, dict) and 'date' in comment:
            try:
                comment_date = to_timestamp(safe_extract(comment['date']))
                if comment_date and (latest_date is None or comment_date > latest_date):
                    latest_date = comment_date
            except:
                continue
    return latest_date

def check_has_acquisition(acquisition_obj):
    """Check if acquisition object exists and has data"""
    acquisition_obj = safe_extract(acquisition_obj)
    return acquisition_obj is not None and isinstance(acquisition_obj, dict)

# --- Orders Helpers ---
def extract_bag_totals(content_obj):
    """Extract total bags from content.bagList structure"""
    content_obj = safe_extract(content_obj)
    if not content_obj or not isinstance(content_obj, dict):
        return 0
    
    bag_list = safe_extract(content_obj.get('bagList', {}))
    if not isinstance(bag_list, dict):
        return 0
    
    total = 0
    for bag_size, meats in bag_list.items():
        if isinstance(meats, dict):
            for meat_count in meats.values():
                total += safe_to_int(meat_count) or 0
    return total

def extract_meat_totals(content_obj, meat_type):
    """Extract total bags for specific meat type from content.bagList"""
    content_obj = safe_extract(content_obj)
    if not content_obj or not isinstance(content_obj, dict):
        return 0
    
    bag_list = safe_extract(content_obj.get('bagList', {}))
    if not isinstance(bag_list, dict):
        return 0
    
    total = 0
    for bag_size, meats in bag_list.items():
        if isinstance(meats, dict) and meat_type in meats:
            total += safe_to_int(meats[meat_type]) or 0
    return total

def extract_bag_size_count(content_obj, size):
    """Extract count of bags for specific size from content.bagList"""
    content_obj = safe_extract(content_obj)
    if not content_obj or not isinstance(content_obj, dict):
        return 0
    
    bag_list = safe_extract(content_obj.get('bagList', {}))
    if not isinstance(bag_list, dict):
        return 0
    
    size_data = bag_list.get(str(size), {})
    if isinstance(size_data, dict):
        total = 0
        for count in size_data.values():
            total += safe_to_int(count) or 0
        return total
    return 0

def extract_handlers_count(package_obj):
    """Count handlers from package.handlers array"""
    package_obj = safe_extract(package_obj)
    if not package_obj or not isinstance(package_obj, dict):
        return 0
    
    handlers = safe_extract(package_obj.get('handlers', []))
    return len(handlers) if isinstance(handlers, list) else 0

def count_extras(content_obj):
    """Count extras from content.extras array"""
    content_obj = safe_extract(content_obj)
    if not content_obj or not isinstance(content_obj, dict):
        return 0
    extras = safe_extract(content_obj.get('extras', []))
    return len(extras) if isinstance(extras, list) else 0

def count_additional_extras(content_obj):
    """Count additional extras from content.additionalExtras array"""
    content_obj = safe_extract(content_obj)
    if not content_obj or not isinstance(content_obj, dict):
        return 0
    extras = safe_extract(content_obj.get('additionalExtras', []))
    return len(extras) if isinstance(extras, list) else 0

# --- Payments Helpers ---
def extract_line_items_count(line_items):
    """Count line items"""
    line_items = safe_extract(line_items)
    if not line_items or not isinstance(line_items, list):
        return 0
    return len(line_items)

def extract_total_product_qty(line_items):
    """Sum quantities from line items"""
    line_items = safe_extract(line_items)
    if not line_items or not isinstance(line_items, list):
        return 0
    
    total = 0
    for item in line_items:
        if isinstance(item, dict) and 'qty' in item:
            qty = safe_to_int(item['qty'])
            if qty:
                total += qty
    return total

def extract_total_product_grams(line_items):
    """Sum total grams from line items (qty * unitGrams)"""
    line_items = safe_extract(line_items)
    if not line_items or not isinstance(line_items, list):
        return 0
    
    total = 0
    for item in line_items:
        if isinstance(item, dict) and 'qty' in item and 'unitGrams' in item:
            qty = safe_to_int(item['qty'])
            unit_grams = safe_to_int(item['unitGrams'])
            if qty and unit_grams:
                total += qty * unit_grams
    return total

def extract_line_items_total_amount(line_items):
    """Sum total amount from line items (qty * unitAmount)"""
    line_items = safe_extract(line_items)
    if not line_items or not isinstance(line_items, list):
        return 0.0
    
    total = 0.0
    for item in line_items:
        if isinstance(item, dict) and 'qty' in item and 'unitAmount' in item:
            qty = safe_to_int(item['qty'])
            unit_amount = safe_to_float(item['unitAmount'])
            if qty and unit_amount:
                total += qty * unit_amount
    return total

def extract_products_list(line_items):
    """Extract comma-separated product names"""
    line_items = safe_extract(line_items)
    if not line_items or not isinstance(line_items, list):
        return ""
    
    products = []
    for item in line_items:
        if isinstance(item, dict) and 'product' in item:
            product = safe_extract(item['product'])
            if product:
                products.append(str(product))
    
    return ", ".join(products)

def extract_linked_order_ids(orders_array):
    """Extract comma-separated order IDs"""
    orders_array = safe_extract(orders_array)
    if not orders_array or not isinstance(orders_array, list):
        return ""
    
    order_ids = []
    for order_id in orders_array:
        id_val = safe_extract(order_id)
        if id_val:
            order_ids.append(str(id_val))
    return ", ".join(order_ids)

def extract_refunds_count(refunds_array):
    """Count refunds"""
    refunds_array = safe_extract(refunds_array)
    if not refunds_array or not isinstance(refunds_array, list):
        return 0
    return len(refunds_array)

def extract_total_refund_amount(refunds_array):
    """Sum refund amounts"""
    refunds_array = safe_extract(refunds_array)
    if not refunds_array or not isinstance(refunds_array, list):
        return 0.0
    
    total = 0.0
    for refund in refunds_array:
        if isinstance(refund, dict) and 'amount' in refund:
            amount = safe_to_float(refund['amount'])
            if amount:
                total += amount
    return total

def extract_latest_refund_status(refunds_array):
    """Get status of most recent refund"""
    refunds_array = safe_extract(refunds_array)
    if not refunds_array or not isinstance(refunds_array, list):
        return None
    
    latest_refund = None
    latest_date = None
    
    for refund in refunds_array:
        if isinstance(refund, dict) and 'createdAt' in refund:
            created_at = to_timestamp(safe_extract(refund['createdAt']))
            if created_at and (latest_date is None or created_at > latest_date):
                latest_date = created_at
                latest_refund = refund
    
    return safe_extract(latest_refund.get('status')) if latest_refund else None

def extract_latest_refund_reason(refunds_array):
    """Get reason category of most recent refund"""
    refunds_array = safe_extract(refunds_array)
    if not refunds_array or not isinstance(refunds_array, list):
        return None
    
    latest_refund = None
    latest_date = None
    
    for refund in refunds_array:
        if isinstance(refund, dict) and 'createdAt' in refund:
            created_at = to_timestamp(safe_extract(refund['createdAt']))
            if created_at and (latest_date is None or created_at > latest_date):
                latest_date = created_at
                latest_refund = refund
    
    if latest_refund and 'reason' in latest_refund:
        reason = safe_extract(latest_refund['reason'])
        if isinstance(reason, dict):
            return safe_extract(reason.get('category'))
    return None

# --- Deliveries Helpers ---
def has_label_data(label_data):
    """Check if labelData exists and is not empty"""
    label_data = safe_extract(label_data)
    return label_data is not None and len(str(label_data)) > 0

def get_label_data_length(label_data):
    """Get length of labelData string"""
    label_data = safe_extract(label_data)
    if label_data is None:
        return 0
    return len(str(label_data))

# --- Leads Archive Helpers ---
def extract_sales_status(sales_obj):
    """Extract status from sales object"""
    sales_obj = safe_extract(sales_obj)
    if not sales_obj or not isinstance(sales_obj, dict):
        return None
    return safe_extract(sales_obj.get('status'))

def extract_sales_assigned_at(sales_obj):
    """Extract assignedAt timestamp from sales object"""
    sales_obj = safe_extract(sales_obj)
    if not sales_obj or not isinstance(sales_obj, dict):
        return None
    return safe_to_timestamp(sales_obj.get('assignedAt'))

def extract_sales_reassignment_count(sales_obj):
    """Extract reassignmentCount from sales object"""
    sales_obj = safe_extract(sales_obj)
    if not sales_obj or not isinstance(sales_obj, dict):
        return 0
    return safe_to_int(sales_obj.get('reassignmentCount', 0)) or 0

def extract_sales_comments_count(sales_obj):
    """Count comments from sales object"""
    sales_obj = safe_extract(sales_obj)
    if not sales_obj or not isinstance(sales_obj, dict):
        return 0
    comments = safe_extract(sales_obj.get('comments', []))
    return len(comments) if isinstance(comments, list) else 0

# --- Contacts Logs Helpers ---
def extract_logs_count(logs_array):
    """Count number of communication logs"""
    logs_array = safe_extract(logs_array)
    if not logs_array or not isinstance(logs_array, list):
        return 0
    return len(logs_array)

def extract_last_log_type(logs_array):
    """Extract event type of the last log entry"""
    logs_array = safe_extract(logs_array)
    if not logs_array or not isinstance(logs_array, list) or len(logs_array) == 0:
        return None
    
    last_log = logs_array[-1]
    if isinstance(last_log, dict) and 'eventType' in last_log:
        return safe_extract(last_log['eventType'])
    return None

def extract_last_log_direction(logs_array):
    """Extract direction of the last log entry"""
    logs_array = safe_extract(logs_array)
    if not logs_array or not isinstance(logs_array, list) or len(logs_array) == 0:
        return None
    
    last_log = logs_array[-1]
    if isinstance(last_log, dict) and 'direction' in last_log:
        return safe_extract(last_log['direction'])
    return None

def extract_last_log_status(logs_array):
    """Extract status of the last log entry"""
    logs_array = safe_extract(logs_array)
    if not logs_array or not isinstance(logs_array, list) or len(logs_array) == 0:
        return None
    
    last_log = logs_array[-1]
    if isinstance(last_log, dict) and 'status' in last_log:
        return safe_extract(last_log['status'])
    return None

def extract_last_log_agent(logs_array):
    """Extract agent ID of the last log entry"""
    logs_array = safe_extract(logs_array)
    if not logs_array or not isinstance(logs_array, list) or len(logs_array) == 0:
        return None
    
    last_log = logs_array[-1]
    if isinstance(last_log, dict) and 'agent' in last_log:
        return safe_extract(last_log['agent'])
    return None

def extract_last_log_timestamp(logs_array):
    """Extract timestamp of the last log entry"""
    logs_array = safe_extract(logs_array)
    if not logs_array or not isinstance(logs_array, list) or len(logs_array) == 0:
        return None
    
    last_log = logs_array[-1]
    if isinstance(last_log, dict) and 'startedAt' in last_log:
        return safe_to_timestamp(last_log['startedAt'])
    return None

def extract_last_log_duration(logs_array):
    """Extract duration of the last log entry"""
    logs_array = safe_extract(logs_array)
    if not logs_array or not isinstance(logs_array, list) or len(logs_array) == 0:
        return None
    
    last_log = logs_array[-1]
    if isinstance(last_log, dict) and 'duration' in last_log:
        return safe_to_int(last_log['duration'])
    return None

def extract_total_duration(logs_array):
    """Sum duration from all log entries"""
    logs_array = safe_extract(logs_array)
    if not logs_array or not isinstance(logs_array, list):
        return 0
    
    total = 0
    for log in logs_array:
        if isinstance(log, dict) and 'duration' in log:
            duration = safe_to_int(log['duration'])
            if duration:
                total += duration
    return total

def extract_call_count(logs_array):
    """Count number of calls in logs"""
    logs_array = safe_extract(logs_array)
    if not logs_array or not isinstance(logs_array, list):
        return 0
    
    count = 0
    for log in logs_array:
        if isinstance(log, dict) and safe_extract(log.get('eventType')) == 'call':
            count += 1
    return count

def extract_email_count(logs_array):
    """Count number of emails in logs"""
    logs_array = safe_extract(logs_array)
    if not logs_array or not isinstance(logs_array, list):
        return 0
    
    count = 0
    for log in logs_array:
        if isinstance(log, dict) and safe_extract(log.get('eventType')) == 'email':
            count += 1
    return count

def extract_sms_count(logs_array):
    """Count number of SMS in logs"""
    logs_array = safe_extract(logs_array)
    if not logs_array or not isinstance(logs_array, list):
        return 0
    
    count = 0
    for log in logs_array:
        if isinstance(log, dict) and safe_extract(log.get('eventType')) == 'sms':
            count += 1
    return count

# --- Retentions Helpers ---
def extract_contact_channels_count(channels_array):
    """Count number of contact channels"""
    channels_array = safe_extract(channels_array)
    if not channels_array or not isinstance(channels_array, list):
        return 0
    return len(channels_array)

def extract_contact_channels_list(channels_array):
    """Extract comma-separated list of contact channels"""
    channels_array = safe_extract(channels_array)
    if not channels_array or not isinstance(channels_array, list):
        return ""
    channels = []
    for channel in channels_array:
        ch = safe_extract(channel)
        if ch:
            channels.append(str(ch))
    return ", ".join(channels)

def extract_reason_category(reason_obj):
    """Extract category from reasonForPause object"""
    reason_obj = safe_extract(reason_obj)
    if not reason_obj or not isinstance(reason_obj, dict):
        return None
    return safe_extract(reason_obj.get('category'))

def extract_reason_subcategory(reason_obj):
    """Extract subcategory from reasonForPause object"""
    reason_obj = safe_extract(reason_obj)
    if not reason_obj or not isinstance(reason_obj, dict):
        return None
    return safe_extract(reason_obj.get('subcategory'))

def extract_comeback_probability(reason_obj):
    """Extract comeback probability from reasonForPause object"""
    reason_obj = safe_extract(reason_obj)
    if not reason_obj or not isinstance(reason_obj, dict):
        return None
    prob = safe_extract(reason_obj.get('comebackProbab'))
    return safe_to_float(prob) if prob is not None else None

# --- Notifications Helpers ---
def is_read(read_at):
    """Determine if notification has been read based on readAt timestamp"""
    return safe_extract(read_at) is not None

# --- Changelogs Helpers ---
def extract_entity_type(entity_id):
    """Extract entity type from ID prefix (ord_, cust_, pay_, etc)"""
    entity_id = safe_extract(entity_id)
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
    logs_array = safe_extract(logs_array)
    if not logs_array or not isinstance(logs_array, list):
        return 0
    
    count = 0
    for log in logs_array:
        if isinstance(log, dict) and safe_extract(log.get('updatedBy')) == actor_name:
            count += 1
    return count

def get_latest_change_info(logs_array):
    """Get info about the most recent change"""
    logs_array = safe_extract(logs_array)
    if not logs_array or not isinstance(logs_array, list) or len(logs_array) == 0:
        return None, None, None
    
    # Sort by createdAt to get latest
    sorted_logs = sorted(logs_array, 
                        key=lambda x: safe_extract(x.get('createdAt', '')) or '', 
                        reverse=True)
    
    if sorted_logs:
        latest = sorted_logs[0]
        return (safe_to_timestamp(latest.get('createdAt')),
                safe_extract(latest.get('updatedBy')),
                safe_extract(latest.get('key')))
    
    return None, None, None

def extract_top_changed_fields(logs_array, top_n=3):
    """Extract the most frequently changed fields"""
    logs_array = safe_extract(logs_array)
    if not logs_array or not isinstance(logs_array, list):
        return ""
    
    field_counts = {}
    for log in logs_array:
        if isinstance(log, dict) and 'key' in log:
            key = safe_extract(log['key'])
            if key:
                field_counts[key] = field_counts.get(key, 0) + 1
    
    if not field_counts:
        return ""
    
    sorted_fields = sorted(field_counts.items(), key=lambda x: x[1], reverse=True)
    top_fields = [field for field, count in sorted_fields[:top_n]]
    return ", ".join(top_fields)

def count_unique_fields(logs_array):
    """Count unique fields that were changed"""
    logs_array = safe_extract(logs_array)
    if not logs_array or not isinstance(logs_array, list):
        return 0
    
    unique_fields = set()
    for log in logs_array:
        if isinstance(log, dict) and 'key' in log:
            key = safe_extract(log['key'])
            if key:
                unique_fields.add(key)
    
    return len(unique_fields)

# --- Stats Helpers ---
def extract_stat_type(stat_id):
    """Extract stat type from ID pattern"""
    stat_id = safe_extract(stat_id)
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
    stat_id = safe_extract(stat_id)
    if not stat_id or not isinstance(stat_id, str):
        return None
    
    parts = stat_id.split('-')
    if len(parts) >= 3:
        return parts[2]
    return None

# --- Sysusers Helpers ---
def join_array_as_string(arr):
    """Join array elements as comma-separated string"""
    arr = safe_extract(arr)
    if not arr or not isinstance(arr, list):
        return ""
    items = []
    for item in arr:
        item_val = safe_extract(item)
        if item_val:
            items.append(str(item_val))
    return ", ".join(items)

def check_nested_field_exists(obj, field_path):
    """Check if a nested field exists"""
    return safe_nested_extract(obj, field_path) is not None

# ============================================================================
# MAPPING DEFINITIONS - ALL COLLECTIONS WITH SAFE EXTRACTION
# ============================================================================

# Customers mapping - COMPLETE WITH ARRAY HANDLING
CUSTOMERS_MAPPING = {
    # Primary Keys & Metadata
    'pk_client': ('_id', to_string),
    'pk_yearmonth': (lambda: datetime.now().strftime("%Y%m"), None),
    'des_data_origin': (Literal('customers'), None),

    # Timestamps - Using safe field extractors for nested fields
    'ts_customer_created_at': (safe_field_extractor('createdAt', to_timestamp), None),
    'ts_updated_at': (safe_field_extractor('updatedAt', to_timestamp), None),
    'ts_last_session': (safe_field_extractor('lastSessionAt', to_timestamp), None),

    # Customer Information
    'des_email': ('email', safe_extract),
    'des_phone': ('phone', safe_extract),
    'des_given_name': ('givenName', safe_extract),
    'des_family_name': ('familyName', safe_extract),
    'des_country': ('country', safe_extract),
    'txt_address': (safe_field_extractor('address.line1'), None),
    'txt_address_aux': (safe_field_extractor('address.line2'), None),
    'des_locality': (safe_field_extractor('address.locality'), None),
    'cod_zip': (safe_field_extractor('address.zip'), None),
    'des_country_address': (safe_field_extractor('address.country'), None),

    # Trial Data
    'imp_trial_amount': (safe_field_extractor('trial.amount', to_float), None),
    'imp_trial_discount': (safe_field_extractor('trial.discount', to_float), None),
    'val_trial_total_daily_grams': (safe_field_extractor('trial.consolidatedTrial.totalDailyGrams', to_int), None),
    'val_trial_bag_count': (safe_field_extractor('trial.consolidatedTrial.bagCount', to_int), None),
    'fk_trial_sales_agent': (safe_field_extractor('trial.salesAgent'), None),
    'des_trial_source': (safe_field_extractor('trial.source'), None),

    # Subscription Data - All using safe extractors
    'des_subscription_status': (safe_field_extractor('subscription.status'), None),
    'ts_subscription_status_updated': (safe_field_extractor('subscription.statusUpdatedAt', to_timestamp), None),
    'fk_subscription_status_updated_by': (safe_field_extractor('subscription.statusUpdatedBy'), None),
    'fk_stripe_customer': (safe_field_extractor('subscription.stripeCustId'), None),
    'cod_card_last4': (safe_field_extractor('subscription.cardLast4'), None),
    'imp_subscription_amount': (safe_field_extractor('subscription.amount', to_float), None),
    'imp_extras_amount': (safe_field_extractor('subscription.extrasAmount', to_float), None),
    'val_orders_in_cycle': (safe_field_extractor('subscription.ordersInCycle', to_int), None),
    'val_payment_cycle_weeks': (safe_field_extractor('subscription.paymentCycleWeeks', to_int), None),
    'val_total_daily_grams': (safe_field_extractor('subscription.totalDailyGrams', to_int), None),
    'val_payment_issues_count': (safe_field_extractor('subscription.paymentIssuesCount', to_int), None),
    'des_delivery_company': (safe_field_extractor('subscription.deliveryCompany'), None),
    'val_cooling_packs_qty': (safe_field_extractor('subscription.coolingPacksQty', to_int), None),
    'pct_computed_discount': (safe_field_extractor('subscription.computedDiscountPercent', to_float), None),
    'des_payment_method_id': (safe_field_extractor('subscription.paymentMethodId'), None),
    'des_payment_method_type': (safe_field_extractor('subscription.paymentMethodType'), None),
    'val_paid_orders_count': (safe_field_extractor('subscription.paidOrders.count', to_int), None),
    'imp_paid_orders_total': (safe_field_extractor('subscription.paidOrders.totalAmount', to_float), None),
    'flg_is_mixed_plan': (safe_field_extractor('subscription.isMixedPlan', to_bool), None),
    'ts_first_mixed_plan': (safe_field_extractor('subscription.firstMixedPlanAt', to_timestamp), None),
    'ts_subscription_paused': (safe_field_extractor('subscription.pausedAt', to_timestamp), None),

    # Pause Reason - Using safe extractors
    'des_pause_reason_category': (safe_field_extractor('subscription.reasonForPause.category'), None),
    'des_pause_reason_subcategory': (safe_field_extractor('subscription.reasonForPause.subcategory'), None),
    'val_paused_count': (safe_field_extractor('subscription.pausedCount', to_int), None),

    # Coupon Data
    'cod_coupon': (safe_field_extractor('subscription.coupon.code'), None),
    'val_referral_count': (safe_field_extractor('subscription.coupon.referralCount', to_int), None),
    'pct_coupon_discount': (safe_field_extractor('subscription.coupon.discountPercent', to_float), None),

    # Active Records
    'val_active_orders_count': (lambda doc: count_array_items(safe_nested_extract(doc, 'subscription.activeOrders')), None),
    'cod_active_payment': (safe_field_extractor('subscription.activePayment'), None),
    'cod_new_cycle_after_order': (safe_field_extractor('subscription.newCycleAfterOrder'), None),

    # Flags
    'flg_review_invitation_pending': (safe_field_extractor('subscription.isReviewInvitationPending', to_bool), None),
    'des_last_review_invitation': (safe_field_extractor('subscription.lastReviewInvitation'), None),
    'flg_contacted_after_status_update': (safe_field_extractor('subscription.isContactedAfterStatusUpdated', to_bool), None),

    # Integration flags
    'flg_updated_on_hubspot': ('isUpdatedOnHubspot', safe_to_bool),
    'flg_updated_on_iterable': ('isUpdatedOnIterable', safe_to_bool),

    # Legacy and Optional
    'val_legacy_id': ('legacyId', safe_to_int),

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

# Leads mapping - COMPLETE WITH ARRAY HANDLING
LEADS_MAPPING = {
    # Primary Keys & Metadata
    'pk_client': ('_id', to_string),
    'pk_yearmonth': (lambda: datetime.now().strftime("%Y%m"), None),
    'des_data_origin': (Literal('leads'), None),

    # Timestamps
    'ts_lead_created_at': ('createdAt', safe_to_timestamp),
    'ts_lead_updated_at': ('leadUpdatedAt', safe_to_timestamp),
    'ts_updated_at': ('updatedAt', safe_to_timestamp),
    'ts_trial_delivery_default': ('defaultDeliveryDate', safe_to_timestamp),
    'ts_trial_delivery': ('trialDeliveryDate', safe_to_timestamp),
    'ts_sales_assigned': (safe_field_extractor('sales.assignedAt', to_timestamp), None),
    'ts_sales_state_updated': (safe_field_extractor('sales.stateUpdatedAt', to_timestamp), None),

    # Lead Information
    'des_email': ('email', safe_extract),
    'des_phone': ('phone', safe_extract),
    'des_given_name': ('givenName', safe_extract),
    'des_family_name': ('familyName', safe_extract),
    'des_country': ('country', safe_extract),
    'cod_zip': ('zip', safe_extract),
    'txt_address': (safe_field_extractor('address.line1'), None),
    'txt_address_aux': (safe_field_extractor('address.line2'), None),
    'des_locality': (safe_field_extractor('address.locality'), None),
    'des_country_address': (safe_field_extractor('address.country'), None),

    # Sales Data
    'des_sales_status': (safe_field_extractor('sales.status'), None),
    'fk_sales_agent': (safe_field_extractor('sales.assignedTo'), None),
    'val_assignment_count': (safe_field_extractor('sales.assignmentCount', to_int), None),
    'val_usage_count': ('usageCount', safe_to_int),
    'txt_not_interested_reason': (safe_field_extractor('sales.notInterestedReason.category'), None),
    'des_not_interested_subcategory': (safe_field_extractor('sales.notInterestedReason.subcategory'), None),

    # Financial Data
    'imp_trial_amount': ('trialAmount', safe_to_float),
    'imp_trial_discount': ('trialDiscount', safe_to_float),
    'imp_subscription_amount': ('subscriptionAmount', safe_to_float),
    'imp_subscription_discount': ('subscriptionDiscount', safe_to_float),
    'pct_subscription_discount': ('subscriptionDiscountPercent', safe_to_float),
    'val_orders_in_cycle': ('ordersInCycle', safe_to_int),

    # Acquisition
    'des_acquisition_source_first': (safe_field_extractor('acquisition.first.source'), None),
    'des_acquisition_source_last': (safe_field_extractor('acquisition.last.source'), None),

    # Subscription data
    'fk_stripe_customer_lead': (safe_field_extractor('subscription.stripeCustId'), None),
    'cod_pricing_factor': (safe_field_extractor('subscription.pricingFactor'), None),
    'ts_initial_payment_attempted': (safe_field_extractor('subscription.initialPaymentAttemptedAt', to_timestamp), None),
    'cod_stripe_payment_id': (safe_field_extractor('subscription.stripePaymentId'), None),

    # Flags & Features
    'flg_mixed_plan': ('isMixedPlan', safe_to_bool),
    'flg_anonymous': ('isAnonymous', safe_to_bool),
    'flg_contact_by_sms': ('contactBySMS', safe_to_bool),
    'flg_updated_on_iterable': ('isUpdatedOnIterable', safe_to_bool),
    'flg_updated_on_hubspot': ('isUpdatedOnHubspot', safe_to_bool),

    # Optional fields
    'cod_coupon': ('coupon', safe_extract),
    'cod_campaign_id': ('campaignId', safe_extract),

    # Dogs aggregated
    'val_dogs_count': ('dogs', count_array_items),

    # Payment log
    'flg_has_payment_log': ('paymentLog', lambda x: safe_extract(x) is not None),
    
    # Standard
    'des_source': (Literal('mongo'), None),
}

# Orders mapping - COMPLETE WITH ARRAY HANDLING  
ORDERS_MAPPING = {
    # Primary Keys & Metadata
    'pk_order': ('_id', to_string),
    'pk_yearmonth': (lambda: datetime.now().strftime("%Y%m"), None),
    'des_data_origin': (Literal('orders'), None),

    # Order Identification
    'fk_customer': ('custId', safe_extract),
    'cod_payment': ('payment', safe_extract),
    'des_full_name': ('fullName', safe_extract),
    
    # Timestamps
    'ts_order_created': ('createdAt', safe_to_timestamp),
    'ts_order_updated': ('updatedAt', safe_to_timestamp),
    'ts_delivery_date': ('deliveryDate', safe_to_timestamp),
    'ts_tentative_delivery_date': ('tentativeDeliveryDate', safe_to_timestamp),

    # Order Status & Classification
    'des_order_status': ('status', safe_extract),
    'des_country': ('country', safe_extract),
    
    # Address Information
    'txt_address_line1': (safe_field_extractor('address.line1'), None),
    'txt_address_line2': (safe_field_extractor('address.line2'), None),
    'des_locality': (safe_field_extractor('address.locality'), None),
    'cod_zip': (safe_field_extractor('address.zip'), None),
    'des_address_country': (safe_field_extractor('address.country'), None),
    
    # Contact Info
    'des_email': ('email', safe_extract),
    'des_phone': ('phone', safe_extract),

    # Order Characteristics - Boolean Flags
    'flg_is_trial': ('isTrial', safe_to_bool),
    'flg_is_secondary': ('isSecondary', safe_to_bool),
    'flg_is_last_in_cycle': ('isLastInCycle', safe_to_bool),
    'flg_is_for_robots': ('isForRobots', safe_to_bool),
    'flg_is_additional': ('isAdditional', safe_to_bool),
    'flg_is_first_renewal': ('isFirstRenewal', safe_to_bool),
    'flg_is_mixed_plan': ('isMixedPlan', safe_to_bool),
    'flg_is_rescheduled': ('isRescheduled', safe_to_bool),
    'flg_is_express_delivery': ('isExpressDelivery', safe_to_bool),
    'flg_has_additional_ice_bags': ('hasAdditionalIceBags', safe_to_bool),
    'flg_is_agency_pickup': ('isAgencyPickup', safe_to_bool),
    'flg_address_is_locked': ('addressIsLocked', safe_to_bool),
    'flg_notification_sent': ('notificationSent', safe_to_bool),

    # Package Details
    'val_cooling_packs_qty': ('coolingPacksQty', safe_to_int),
    'val_package_bag_count': (safe_field_extractor('package.bagCount', to_int), None),
    'val_total_package_count': (safe_field_extractor('package.totalPackageCount', to_int), None),
    'val_total_weight_kg': (safe_field_extractor('package.totalWeightKg', to_float), None),
    'val_package_handlers_count': ('package', extract_handlers_count),
    'flg_package_has_issue': (safe_field_extractor('package.hasIssue', to_bool), None),
    'des_package_issue_category': (safe_field_extractor('package.issueType'), None),
    'val_delta_days': ('deltaDays', safe_to_int),
    'des_additional_order_reason': ('additionalOrderReason', safe_extract),
    'des_locked_by': ('lockedBy', safe_extract),

    # Delivery Details
    'des_delivery_company': (safe_field_extractor('delivery.deliveryCompany'), None),
    'flg_delivery_has_issue': (safe_field_extractor('delivery.hasIssue', to_bool), None),
    'des_delivery_issue_category': (safe_field_extractor('delivery.issueType'), None),
    'cod_tracking_url': (safe_field_extractor('delivery.trackingUrl'), None),
    'cod_parcel_id': (safe_field_extractor('delivery.parcelId'), None),
    'des_label_group': (safe_field_extractor('delivery.labelGroup'), None),

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

# Payments mapping - COMPLETE WITH ARRAY HANDLING
PAYMENTS_MAPPING = {
    # Primary Keys & Metadata
    'pk_payment': ('_id', to_string),
    'pk_yearmonth': (lambda: datetime.now().strftime("%Y%m"), None),
    'des_data_origin': (Literal('payments'), None),

    # Payment Identification & Linking
    'fk_customer': ('custId', safe_extract),
    'val_linked_orders_count': ('orders', count_array_items),
    'txt_linked_order_ids': ('orders', extract_linked_order_ids),
    
    # Timestamps
    'ts_payment_date': ('date', safe_to_timestamp),
    'ts_payment_created': ('createdAt', safe_to_timestamp),
    'ts_payment_updated': ('updatedAt', safe_to_timestamp),

    # Payment Status & Processing
    'des_payment_status': ('status', safe_extract),
    'des_country': ('country', safe_extract),
    'val_failed_attempts_count': ('failedAttemptsCount', safe_to_int),
    'des_error_code': ('errorCode', safe_extract),

    # Financial Amounts
    'imp_payment_amount': ('amount', safe_to_float),
    'imp_invoice_amount': ('invoiceAmount', safe_to_float),
    'imp_discount_amount': ('discount', safe_to_float),
    'pct_discount_percent': ('discountPercent', safe_to_float),
    'imp_extras_amount': ('extrasAmount', safe_to_float),
    'imp_shipping_amount': ('shippingAmount', safe_to_float),
    'imp_additional_delivery_amount': ('additionalDeliveryAmount', safe_to_float),

    # Stripe Integration
    'fk_stripe_customer': ('stripeCustId', safe_extract),
    'cod_stripe_payment_id': ('stripePaymentId', safe_extract),
    'cod_stripe_charge_id': ('stripeChargeId', safe_extract),

    # Payment Method
    'des_payment_method_type': ('paymentMethodType', safe_extract),
    'cod_card_last4': ('cardLast4', safe_extract),

    # Discounts Applied
    'pct_subscription_discount_applied': (safe_field_extractor('discountsApplied.subscriptionDiscountPercent', to_float), None),
    'val_referral_count_applied': (safe_field_extractor('discountsApplied.referralCount', to_int), None),
    'cod_applied_coupon': (safe_field_extractor('discountsApplied.appliedCoupon'), None),
    'pct_trial_discount_applied': (safe_field_extractor('discountsApplied.trialDiscountPercent', to_float), None),

    # Payment Characteristics - Boolean Flags
    'flg_is_trial': ('isTrial', safe_to_bool),
    'flg_is_first_renewal': ('isFirstRenewal', safe_to_bool),
    'flg_is_rescheduled': ('isRescheduled', safe_to_bool),
    'flg_is_additional': ('isAdditional', safe_to_bool),
    'flg_is_legacy': ('isLegacy', safe_to_bool),
    'flg_renewal_email_sent': ('isRenewalEmailSent', safe_to_bool),

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
    'cod_pricing_factor': ('pricingFactor', safe_extract),
    'cod_coupon': ('coupon', safe_extract),

    # Standard
    'des_source': (Literal('mongo'), None),
}

# Continue with remaining collections using safe extractors...
# Stats mapping - FIXED WITH SAFE EXTRACTORS
STATS_MAPPING = {
    'pk_stat': ('_id', to_string),
    'pk_yearmonth': (lambda: datetime.now().strftime("%Y%m"), None),
    'des_data_origin': (Literal('stats'), None),
    'ts_created_at': ('createdAt', safe_to_timestamp),
    'ts_updated_at': ('updatedAt', safe_to_timestamp),
    'val_year': ('year', safe_to_int),
    'val_month': ('month', safe_to_int),
    'des_stat_type': ('_id', extract_stat_type),
    'fk_agent_or_handler': ('_id', extract_agent_from_stat_id),
    'des_stat_country': ('country', safe_extract),  # FIX: Using safe_extract
    'val_total_assigned_leads': (safe_field_extractor('totals.assignedLeads.total', to_int), None),  # FIX: Using safe_field_extractor
    'val_total_appointments': (safe_field_extractor('totals.appointments', to_int), None),  # FIX
    'val_total_sales': (safe_field_extractor('totals.sales', to_int), None),  # FIX
    'val_total_not_answered': (safe_field_extractor('totals.notAnswered', to_int), None),  # FIX
    'val_total_not_interested': (safe_field_extractor('totals.notInterested', to_int), None),  # FIX
    'val_total_orders': (safe_field_extractor('totals.orderCount', to_int), None),  # FIX
    'val_total_packages': (safe_field_extractor('totals.packageCount', to_int), None),  # FIX
    'pct_conversion_rate': ('conversionRate', safe_to_float),
    'pct_retention_rate': ('retentionRate', safe_to_float),
    'imp_average_sales': ('averageSales', safe_to_float),
    'des_source': (Literal('mongo'), None),
}

# Apply safe extractors to remaining collections...
# For brevity, I'll show the pattern for a few more critical ones:

# Deliveries mapping - WITH SAFE EXTRACTORS
DELIVERIES_MAPPING = {
    'pk_delivery': ('_id', to_string),
    'pk_yearmonth': (lambda: datetime.now().strftime("%Y%m"), None),
    'des_data_origin': (Literal('deliveries'), None),
    'fk_customer': ('custId', safe_extract),
    'des_full_name': ('fullName', safe_extract),
    'cod_parcel_id': ('parcelId', safe_extract),
    'ts_delivery_created': ('createdAt', safe_to_timestamp),
    'ts_delivery_updated': ('updatedAt', safe_to_timestamp),
    'ts_delivery_date_scheduled': ('deliveryDate', safe_to_timestamp),
    'ts_delivery_date_actual': ('date', safe_to_timestamp),
    'ts_issue_solved_at': (safe_field_extractor('issue.solvedAt', to_timestamp), None),
    'des_delivery_status': ('status', safe_extract),
    'des_country': ('country', safe_extract),
    'des_delivery_company': ('deliveryCompany', safe_extract),
    'des_label_group': ('labelGroup', safe_extract),
    'txt_address_line1': (safe_field_extractor('address.line1'), None),
    'txt_address_line2': (safe_field_extractor('address.line2'), None),
    'des_locality': (safe_field_extractor('address.locality'), None),
    'cod_zip': (safe_field_extractor('address.zip'), None),
    'des_address_country': (safe_field_extractor('address.country'), None),
    'flg_is_for_robots': ('isForRobots', safe_to_bool),
    'flg_cust_label_printed': (safe_field_extractor('isPrinted.cust', to_bool), None),
    'flg_internal_label_printed': (safe_field_extractor('isPrinted.internal', to_bool), None),
    'flg_has_issue': (safe_field_extractor('issue.hasIssue', to_bool), None),
    'txt_issue_reason': (safe_field_extractor('issue.reason'), None),
    'flg_has_label_data': ('labelData', has_label_data),
    'val_label_data_length': ('labelData', get_label_data_length),
    'flg_has_internal_label_data': ('internalLabelData', has_label_data),
    'val_internal_label_data_length': ('internalLabelData', get_label_data_length),
    'des_source': (Literal('mongo'), None),
}

# Apply the same pattern to all other mappings...
# The key changes are:
# 1. All simple fields use safe_extract or safe_to_* functions
# 2. All nested fields use safe_field_extractor
# 3. All custom extractors already use safe_extract internally

# I'll include the remaining mappings with safe extractors:

COUPONS_MAPPING = {
    'pk_coupon': ('_id', to_string),
    'pk_yearmonth': (lambda: datetime.now().strftime("%Y%m"), None),
    'des_data_origin': (Literal('coupons'), None),
    'cod_coupon': ('_id', to_string),
    'ts_coupon_created_at': ('createdAt', safe_to_timestamp),
    'des_coupon_type': ('type', safe_extract),
    'des_country': ('country', safe_extract),
    'flg_is_not_applicable': ('isNotApplicable', safe_to_bool),
    'des_source': (Literal('mongo'), None),
}

USERS_METADATA_MAPPING = {
    'pk_user_metadata': ('_id', to_string),
    'pk_yearmonth': (lambda: datetime.now().strftime("%Y%m"), None),
    'des_data_origin': (Literal('users-metadata'), None),
    'fk_customer': ('_id', to_string),
    'txt_password_hash': ('password', safe_extract),
    'cod_verification_token': ('verificationToken', safe_extract),
    'ts_auth_created_at': ('createdAt', safe_to_timestamp),
    'ts_auth_updated_at': ('updatedAt', safe_to_timestamp),
    'val_version': ('__v', safe_to_int),
    'flg_is_suspended': ('isSuspended', safe_to_bool),
    'des_source': (Literal('mongo'), None),
}

LEADS_ARCHIVE_MAPPING = {
    'pk_lead_archive': ('_id', to_string),
    'pk_yearmonth': (lambda: datetime.now().strftime("%Y%m"), None),
    'des_data_origin': (Literal('leads-archive'), None),
    'fk_lead': ('_id', to_string),
    'ts_lead_created_at': ('createdAt', safe_to_timestamp),
    'ts_lead_updated_at': ('leadUpdatedAt', safe_to_timestamp),
    'ts_updated_at': ('updatedAt', safe_to_timestamp),
    'ts_trial_delivery_default': ('defaultDeliveryDate', safe_to_timestamp),
    'ts_trial_delivery': ('trialDeliveryDate', safe_to_timestamp),
    'des_email': ('email', safe_extract),
    'des_phone': ('phone', safe_extract),
    'des_given_name': ('givenName', safe_extract),
    'des_family_name': ('familyName', safe_extract),
    'des_country': ('country', safe_extract),
    'cod_zip': ('zip', safe_extract),
    'imp_trial_amount': ('trialAmount', safe_to_float),
    'imp_trial_discount': ('trialDiscount', safe_to_float),
    'imp_subscription_amount': ('subscriptionAmount', safe_to_float),
    'imp_subscription_discount': ('subscriptionDiscount', safe_to_float),
    'pct_subscription_discount': ('subscriptionDiscountPercent', safe_to_float),
    'val_orders_in_cycle': ('ordersInCycle', safe_to_int),
    'val_usage_count': ('usageCount', safe_to_int),
    'val_mailing_stage': ('mailingStage', safe_to_int),
    'cod_coupon': ('coupon', safe_extract),
    'cod_campaign_id': ('campaignId', safe_extract),
    'flg_has_email_deliverability': ('emailDeliverability', lambda x: safe_extract(x) is not None),
    'des_sales_status': ('sales', extract_sales_status),
    'ts_sales_assigned_at': ('sales', extract_sales_assigned_at),
    'val_sales_reassignment_count': ('sales', extract_sales_reassignment_count),
    'val_sales_comments_count': ('sales', extract_sales_comments_count),
    'val_dogs_count': ('dogs', count_array_items),
    'txt_dog_names': ('dogs', extract_dog_names),
    'val_total_dog_weight': ('dogs', sum_dog_weights),
    'flg_mixed_plan': ('isMixedPlan', safe_to_bool),
    'flg_anonymous': ('isAnonymous', safe_to_bool),
    'flg_contact_by_sms': ('contactBySMS', safe_to_bool),
    'flg_updated_on_hubspot': ('isUpdatedOnHubspot', safe_to_bool),
    'flg_updated_on_iterable': ('isUpdatedOnIterable', safe_to_bool),
    'flg_has_acquisition_data': ('acquisition', lambda x: safe_extract(x) is not None),
    'flg_has_shared_info': ('sharedInfo', lambda x: safe_extract(x) is not None),
    'flg_has_subscription_data': ('subscription', lambda x: safe_extract(x) is not None),
    'flg_has_address': ('address', lambda x: safe_extract(x) is not None),
    'flg_has_payment_log': ('paymentLog', lambda x: safe_extract(x) is not None),
    'val_version': ('__v', safe_to_int),
    'des_source': (Literal('mongo'), None),
}

CONTACTS_LOGS_MAPPING = {
    'pk_contact_log': ('_id', to_string),
    'pk_yearmonth': (lambda: datetime.now().strftime("%Y%m"), None),
    'des_data_origin': (Literal('contacts-logs'), None),
    'fk_lead': ('_id', to_string),
    'ts_created_at': ('createdAt', safe_to_timestamp),
    'ts_updated_at': ('updatedAt', safe_to_timestamp),
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

RETENTIONS_MAPPING = {
    'pk_retention': ('_id', to_string),
    'pk_yearmonth': (lambda: datetime.now().strftime("%Y%m"), None),
    'des_data_origin': (Literal('retentions'), None),
    'fk_customer': ('cust', to_string),
    'ts_created_at': ('createdAt', safe_to_timestamp),
    'ts_updated_at': ('updatedAt', safe_to_timestamp),
    'ts_assigned_at': ('assignedAt', safe_to_timestamp),
    'ts_paused_at': ('pausedAt', safe_to_timestamp),
    'ts_reactivated_at': ('reactivatedAt', safe_to_timestamp),
    'ts_contacted_at': ('contactedAt', safe_to_timestamp),
    'ts_appointment_at': ('appointmentAt', safe_to_timestamp),
    'des_retention_status': ('status', safe_extract),
    'val_reassignment_count': ('reassignmentCount', safe_to_int),
    'val_contact_channels_count': ('contactChannels', extract_contact_channels_count),
    'txt_contact_channels': ('contactChannels', extract_contact_channels_list),
    'des_pause_reason_category': ('reasonForPause', extract_reason_category),
    'des_pause_reason_subcategory': ('reasonForPause', extract_reason_subcategory),
    'val_comeback_probability': ('reasonForPause', extract_comeback_probability),
    'fk_sys_user': ('sysUser', safe_extract),
    'cod_zendesk_ticket': ('zendeskTicketId', safe_extract),
    'flg_reactivated_by_agent': ('isReactivatedByAgent', safe_to_bool),
    'flg_retention_due_to_agent': ('isRetentionDueToAgent', safe_to_bool),
    'des_source': (Literal('mongo'), None),
}

NOTIFICATIONS_MAPPING = {
    'pk_notification': ('_id', to_string),
    'pk_yearmonth': (lambda: datetime.now().strftime("%Y%m"), None),
    'des_data_origin': (Literal('notifications'), None),
    'fk_recipient': ('recipient', to_string),
    'des_recipient_model': ('recipientModel', safe_extract),
    'fk_document': ('docId', to_string),
    'des_document_model': ('docModel', safe_extract),
    'des_notification_type': ('notificationType', safe_extract),
    'ts_created_at': ('createdAt', safe_to_timestamp),
    'ts_updated_at': ('updatedAt', safe_to_timestamp),
    'ts_read_at': ('readAt', safe_to_timestamp),
    'flg_is_read': ('readAt', is_read),
    'des_source': (Literal('mongo'), None),
}

APPOINTMENTS_MAPPING = {
    'pk_appointment': ('_id', to_string),
    'pk_yearmonth': (lambda: datetime.now().strftime("%Y%m"), None),
    'des_data_origin': (Literal('appointments'), None),
    'fk_sys_user': ('sysUserId', to_string),
    'fk_lead': ('leadId', to_string),
    'ts_created_at': ('createdAt', safe_to_timestamp),
    'ts_updated_at': ('updatedAt', safe_to_timestamp),
    'ts_starts_at': ('startsAt', safe_to_timestamp),
    'txt_notes': ('notes', safe_extract),
    'des_source': (Literal('mongo'), None),
}

CHANGELOGS_MAPPING = {
    'pk_changelog': ('_id', to_string),
    'pk_yearmonth': (lambda: datetime.now().strftime("%Y%m"), None),
    'des_data_origin': (Literal('changelogs'), None),
    'ts_created_at': ('createdAt', safe_to_timestamp),
    'ts_updated_at': ('updatedAt', safe_to_timestamp),
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

ORDERS_ARCHIVE_MAPPING = {
    'pk_order': ('_id', to_string),
    'pk_yearmonth': (lambda: datetime.now().strftime("%Y%m"), None),
    'des_data_origin': (Literal('orders-archive'), None),
    'fk_customer': ('custId', safe_extract),
    'cod_payment': ('payment', safe_extract),
    'des_full_name': ('fullName', safe_extract),
    'ts_order_created': ('createdAt', safe_to_timestamp),
    'ts_order_updated': ('updatedAt', safe_to_timestamp),
    'ts_delivery_date': ('deliveryDate', safe_to_timestamp),
    'ts_initial_date': ('initialDate', safe_to_timestamp),
    'des_order_status': ('status', safe_extract),
    'des_country': ('country', safe_extract),
    'txt_address_line1': (safe_field_extractor('address.line1'), None),
    'txt_address_line2': (safe_field_extractor('address.line2'), None),
    'des_locality': (safe_field_extractor('address.locality'), None),
    'cod_zip': (safe_field_extractor('address.zip'), None),
    'des_address_country': (safe_field_extractor('address.country'), None),
    'des_email': ('email', safe_extract),
    'des_phone': ('phone', safe_extract),
    'flg_is_trial': ('isTrial', safe_to_bool),
    'flg_is_secondary': ('isSecondary', safe_to_bool),
    'flg_is_last_in_cycle': ('isLastInCycle', safe_to_bool),
    'flg_is_additional': ('isAdditional', safe_to_bool),
    'flg_is_first_renewal': ('isFirstRenewal', safe_to_bool),
    'flg_is_rescheduled': ('isRescheduled', safe_to_bool),
    'flg_is_express_delivery': ('isExpressDelivery', safe_to_bool),
    'flg_has_additional_ice_bags': ('hasAdditionalIceBags', safe_to_bool),
    'flg_address_is_locked': ('addressIsLocked', safe_to_bool),
    'flg_is_legacy': ('isLegacy', safe_to_bool),
    'flg_was_moved': ('wasMoved', safe_to_bool),
    'flg_was_moved_back': ('wasMovedBack', safe_to_bool),
    'flg_was_reset': ('wasReset', safe_to_bool),
    'flg_notification_sent': ('notificationSent', safe_to_bool),
    'val_package_bag_count': (safe_field_extractor('package.bagCount', to_int), None),
    'val_total_package_count': (safe_field_extractor('package.totalPackageCount', to_int), None),
    'val_total_weight_kg': (safe_field_extractor('package.totalWeightKg', to_float), None),
    'des_delivery_company': (safe_field_extractor('delivery.deliveryCompany'), None),
    'flg_delivery_has_issue': (safe_field_extractor('delivery.hasIssue', to_bool), None),
    'val_total_bags': (safe_field_extractor('content.bagCount', to_int), None),
    'val_chicken_portions': (safe_field_extractor('content.menu.chicken', to_int), None),
    'val_turkey_portions': (safe_field_extractor('content.menu.turkey', to_int), None),
    'des_locked_by': ('lockedBy', safe_extract),
    'des_updated_by': ('__updatedBy', safe_extract),
    'val_delta_days': ('deltaDays', safe_to_int),
    'val_version': ('__v', safe_to_int),
    'des_source': (Literal('mongo'), None),
}

PAYMENTS_ARCHIVE_MAPPING = {
    'pk_payment': ('_id', to_string),
    'pk_yearmonth': (lambda: datetime.now().strftime("%Y%m"), None),
    'des_data_origin': (Literal('payments-archive'), None),
    'fk_customer': ('custId', safe_extract),
    'val_linked_orders_count': ('orders', count_array_items),
    'txt_linked_order_ids': ('orders', extract_linked_order_ids),
    'ts_payment_date': ('date', safe_to_timestamp),
    'ts_payment_created': ('createdAt', safe_to_timestamp),
    'ts_payment_updated': ('updatedAt', safe_to_timestamp),
    'ts_initial_date': ('initialDate', safe_to_timestamp),
    'des_payment_status': ('status', safe_extract),
    'des_country': ('country', safe_extract),
    'val_failed_attempts_count': ('failedAttemptsCount', safe_to_int),
    'flg_first_attempt_failed': ('firstAttemptFailed', safe_to_bool),
    'imp_payment_amount': ('amount', safe_to_float),
    'imp_invoice_amount': ('invoiceAmount', safe_to_float),
    'imp_discount_amount': ('discount', safe_to_float),
    'pct_discount_percent': ('discountPercent', safe_to_float),
    'imp_extras_amount': ('extrasAmount', safe_to_float),
    'imp_shipping_amount': ('shippingAmount', safe_to_float),
    'fk_stripe_customer': ('stripeCustId', safe_extract),
    'cod_stripe_payment_id': ('stripePaymentId', safe_extract),
    'cod_stripe_invoice_id': ('stripeInvoiceId', safe_extract),
    'cod_stripe_charge_id': ('stripeChargeId', safe_extract),
    'val_line_items_count': ('lineItems', extract_line_items_count),
    'val_total_product_qty': ('lineItems', extract_total_product_qty),
    'val_total_product_grams': ('lineItems', extract_total_product_grams),
    'imp_line_items_total_amount': ('lineItems', extract_line_items_total_amount),
    'flg_is_trial': ('isTrial', safe_to_bool),
    'flg_is_first_renewal': ('isFirstRenewal', safe_to_bool),
    'flg_is_additional': ('isAdditional', safe_to_bool),
    'flg_is_legacy': ('isLegacy', safe_to_bool),
    'flg_was_moved': ('wasMoved', safe_to_bool),
    'cod_coupon': ('coupon', safe_extract),
    'cod_invoice_code': ('invoiceCode', safe_extract),
    'val_version': ('__v', safe_to_int),
    'des_source': (Literal('mongo'), None),
}

PACKAGES_MAPPING = {
    'pk_package': ('_id', to_string),
    'pk_yearmonth': (lambda: datetime.now().strftime("%Y%m"), None),
    'des_data_origin': (Literal('packages'), None),
    'fk_handler': ('handlerId', safe_extract),
    'ts_created_at': ('createdAt', safe_to_timestamp),
    'ts_updated_at': ('updatedAt', safe_to_timestamp),
    'ts_used_at': ('usedAt', safe_to_timestamp),
    'val_bag_count': ('bagCount', safe_to_int),
    'val_daily_grams': ('dailyGrams', safe_to_int),
    'flg_is_trial': ('isTrial', safe_to_bool),
    'flg_is_used': ('usedAt', lambda x: safe_extract(x) is not None),
    'des_source': (Literal('mongo'), None),
}

ENGAGEMENT_HISTORIES_MAPPING = {
    'pk_engagement': ('_id', to_string),
    'pk_yearmonth': (lambda: datetime.now().strftime("%Y%m"), None),
    'des_data_origin': (Literal('engagement-histories'), None),
    'ts_created_at': ('createdAt', safe_to_timestamp),
    'ts_updated_at': ('updatedAt', safe_to_timestamp),
    'flg_survey_engaged': (safe_field_extractor('cust.tests.customerDataSurvey.isEngaged', to_bool), None),
    'des_survey_value': (safe_field_extractor('cust.tests.customerDataSurvey.value'), None),
    'flg_has_daily_grams_recommendations': (lambda doc: safe_nested_extract(doc, 'cust.recommendationData.dailyGrams') is not None and len(safe_extract(safe_nested_extract(doc, 'cust.recommendationData.dailyGrams'), [])) > 0, None),
    'flg_has_menu_recommendations': (lambda doc: safe_nested_extract(doc, 'cust.recommendationData.menus') is not None and len(safe_extract(safe_nested_extract(doc, 'cust.recommendationData.menus'), [])) > 0, None),
    'des_source': (Literal('mongo'), None),
}

GEOCONTEXT_MAPPING = {
    'pk_geocontext': ('_id', to_string),
    'pk_yearmonth': (lambda: datetime.now().strftime("%Y%m"), None),
    'des_data_origin': (Literal('geocontext'), None),
    'val_ip_range_start': ('fromIp', to_string),
    'val_ip_range_end': ('toIp', to_string),
    'des_country': ('country', safe_extract),
    'des_source': (Literal('mongo'), None),
}

INVALID_PHONES_MAPPING = {
    'pk_phone': ('_id', to_string),
    'pk_yearmonth': (lambda: datetime.now().strftime("%Y%m"), None),
    'des_data_origin': (Literal('invalid-phones'), None),
    'des_phone_number': ('_id', to_string),
    'des_country': ('country', safe_extract),
    'des_created_by': ('createdBy', safe_extract),
    'ts_created_at': ('createdAt', safe_to_timestamp),
    'des_source': (Literal('mongo'), None),
}

SYSUSERS_MAPPING = {
    'pk_sysuser': ('_id', to_string),
    'pk_yearmonth': (lambda: datetime.now().strftime("%Y%m"), None),
    'des_data_origin': (Literal('sysusers'), None),
    'des_given_name': ('givenName', safe_extract),
    'des_family_name': ('familyName', safe_extract),
    'des_email': ('email', safe_extract),
    'des_country': ('country', safe_extract),
    'ts_created_at': ('createdAt', safe_to_timestamp),
    'ts_updated_at': ('updatedAt', safe_to_timestamp),
    'ts_last_session_at': ('lastSessionAt', safe_to_timestamp),
    'txt_roles': ('roles', join_array_as_string),
    'des_role': ('role', safe_extract),
    'txt_manages_countries': ('managesCountries', join_array_as_string),
    'flg_is_suspended': ('isSuspended', safe_to_bool),
    'flg_is_sales_available': (safe_field_extractor('sales.isAvailable', to_bool), None),
    'flg_has_sales_tracking': ('sales', lambda x: safe_extract(x) is not None),
    'flg_has_retentions': ('retentions', lambda x: safe_extract(x) is not None),
    'des_source': (Literal('mongo'), None),
}

SYSINFO_MAPPING = {
    'pk_config': ('_id', to_string),
    'pk_yearmonth': (lambda: datetime.now().strftime("%Y%m"), None),
    'des_data_origin': (Literal('sysinfo'), None),
    'ts_updated_at': ('updatedAt', safe_to_timestamp),
    'des_config_type': ('_id', to_string),
    'flg_has_cron_settings': ('cron', lambda x: safe_extract(x) is not None),
    'flg_has_email_settings': ('transactionalEmails', lambda x: safe_extract(x) is not None),
    'flg_has_sales_settings': ('sales', lambda x: safe_extract(x) is not None),
    'flg_has_robot_settings': ('robots', lambda x: safe_extract(x) is not None),
    'flg_has_stock_levels': ('levels', lambda x: safe_extract(x) is not None),
    'flg_has_agent_lists': ('_id', lambda x: 'AVAILABLE-AGENTS' in str(safe_extract(x)) if safe_extract(x) else False),
    'val_chicken_stock': (safe_field_extractor('levels.chicken.300', to_int), None),
    'val_salmon_stock': (safe_field_extractor('levels.salmon.300', to_int), None),
    'val_beef_stock': (safe_field_extractor('levels.beef.300', to_int), None),
    'val_turkey_stock': (safe_field_extractor('levels.turkey.300', to_int), None),
    'des_source': (Literal('mongo'), None),
}

# ============================================================================
# FINAL MAPPINGS DICTIONARY - ALL COLLECTIONS WITH PROPER NAMES
# ============================================================================
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
    'changelogs': CHANGELOGS_MAPPING,
    'orders-archive': ORDERS_ARCHIVE_MAPPING,
    'payments-archive': PAYMENTS_ARCHIVE_MAPPING,
    'packages': PACKAGES_MAPPING,
    'engagement-histories': ENGAGEMENT_HISTORIES_MAPPING,
    'geocontext': GEOCONTEXT_MAPPING,
    'invalid-phones': INVALID_PHONES_MAPPING,
    'sysusers': SYSUSERS_MAPPING,
    'stats': STATS_MAPPING,
    'sysinfo': SYSINFO_MAPPING,
}