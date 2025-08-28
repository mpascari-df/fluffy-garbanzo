import pyarrow as pa

# Leads schema - based on actual MongoDB field analysis (399 docs)
# Focused on lead-specific fields and actual availability
_leads_schema = pa.schema([
    # Primary Keys & Metadata
    pa.field('pk_client', pa.string()),
    pa.field('pk_yearmonth', pa.string()),
    pa.field('des_data_origin', pa.string()),

    # Timestamps - based on actual availability
    pa.field('ts_lead_created_at', pa.timestamp('ns')),
    pa.field('ts_lead_updated_at', pa.timestamp('ns')),
    pa.field('ts_updated_at', pa.timestamp('ns')),
    pa.field('ts_trial_delivery_default', pa.timestamp('ns')),
    pa.field('ts_trial_delivery', pa.timestamp('ns')),  # 71/399 docs
    pa.field('ts_sales_assigned', pa.timestamp('ns')),
    pa.field('ts_sales_state_updated', pa.timestamp('ns')),
    pa.field('ts_payment_attempted', pa.timestamp('ns')),  # From paymentLog

    # Lead Information - actual availability in MongoDB
    pa.field('des_email', pa.string()),  # 379/399
    pa.field('des_phone', pa.string()),  # 378/399
    pa.field('des_given_name', pa.string()),  # 149/399
    pa.field('des_family_name', pa.string()),  # 123/399
    pa.field('des_country', pa.string()),  # 399/399
    pa.field('cod_zip', pa.string()),  # 159/399
    pa.field('txt_address', pa.string()),  # 41/399 from address.line1
    pa.field('txt_address_aux', pa.string()),  # 41/399 from address.line2
    pa.field('des_locality', pa.string()),  # 41/399
    pa.field('des_country_address', pa.string()),  # 41/399

    # Sales Data
    pa.field('des_sales_status', pa.string()),  # 399/399
    pa.field('fk_sales_agent', pa.string()),
    pa.field('val_assignment_count', pa.int64()),
    pa.field('val_usage_count', pa.int64()),  # 399/399
    pa.field('txt_not_interested_reason', pa.string()),
    pa.field('des_not_interested_subcategory', pa.string()),

    # Financial Data - all leads have this
    pa.field('imp_trial_amount', pa.float64()),  # 399/399
    pa.field('imp_trial_discount', pa.float64()),  # 344/399
    pa.field('imp_subscription_amount', pa.float64()),  # 399/399
    pa.field('imp_subscription_discount', pa.float64()),  # 344/399
    pa.field('pct_subscription_discount', pa.float64()),  # 344/399
    pa.field('val_orders_in_cycle', pa.int64()),  # 339/399

    # Acquisition - limited data available
    pa.field('des_acquisition_source_first', pa.string()),
    pa.field('des_acquisition_source_last', pa.string()),

    # Flags & Features
    pa.field('flg_mixed_plan', pa.bool_()),  # 399/399
    pa.field('cod_pricing_factor', pa.string()),  # 218/399
    pa.field('flg_anonymous', pa.bool_()),  # 11/399
    pa.field('flg_contact_by_sms', pa.bool_()),  # 18/399

    # Optional fields
    pa.field('cod_coupon', pa.string()),  # 60/399
    pa.field('cod_campaign_id', pa.string()),  # 147/399
    pa.field('txt_payment_log_source', pa.string()),  # 16/399
    
    # Standard fields for compatibility
    pa.field('des_source', pa.string()),
])

# Customers schema - based on actual MongoDB field analysis (2634 docs)
# Much richer structure than leads
_customers_schema = pa.schema([
    # Primary Keys & Metadata
    pa.field('pk_client', pa.string()),
    pa.field('pk_yearmonth', pa.string()),
    pa.field('des_data_origin', pa.string()),

    # Timestamps
    pa.field('ts_customer_created_at', pa.timestamp('ns')),
    pa.field('ts_updated_at', pa.timestamp('ns')),
    pa.field('ts_last_session', pa.timestamp('ns')),  # 492/2634

    # Customer Information - all customers have complete info
    pa.field('des_email', pa.string()),  # 2634/2634
    pa.field('des_phone', pa.string()),  # 2634/2634
    pa.field('des_given_name', pa.string()),  # 2634/2634
    pa.field('des_family_name', pa.string()),  # 2634/2634
    pa.field('des_country', pa.string()),  # 2634/2634
    pa.field('txt_address', pa.string()),  # 2634/2634 from address.line1
    pa.field('txt_address_aux', pa.string()),  # from address.line2
    pa.field('des_locality', pa.string()),  # 2634/2634
    pa.field('cod_zip', pa.string()),  # 2634/2634
    pa.field('des_country_address', pa.string()),  # 2634/2634

    # Trial Data - all customers have this
    pa.field('imp_trial_amount', pa.float64()),
    pa.field('imp_trial_discount', pa.float64()),
    pa.field('val_trial_total_daily_grams', pa.int64()),
    pa.field('val_trial_bag_count', pa.int64()),

    # Subscription Data - rich structure for customers
    pa.field('des_subscription_status', pa.string()),  # 2634/2634
    pa.field('fk_stripe_customer', pa.string()),  # stripeCustId
    pa.field('cod_card_last4', pa.string()),
    pa.field('imp_subscription_amount', pa.float64()),
    pa.field('val_orders_in_cycle', pa.int64()),
    pa.field('val_payment_cycle_weeks', pa.int64()),
    pa.field('val_total_daily_grams', pa.int64()),
    pa.field('val_payment_issues_count', pa.int64()),
    pa.field('des_delivery_company', pa.string()),
    pa.field('val_cooling_packs_qty', pa.int64()),
    pa.field('pct_computed_discount', pa.float64()),

    # Coupon Data
    pa.field('cod_coupon', pa.string()),
    pa.field('val_referral_count', pa.int64()),
    pa.field('pct_coupon_discount', pa.float64()),

    # Active Records (arrays in MongoDB)
    pa.field('val_active_orders_count', pa.int64()),  # count of activeOrders array
    pa.field('cod_active_payment', pa.string()),

    # Flags
    pa.field('flg_review_invitation_pending', pa.bool_()),
    pa.field('des_last_review_invitation', pa.string()),
    pa.field('flg_contacted_after_pause', pa.bool_()),
    pa.field('flg_contacted_after_status_update', pa.bool_()),

    # Integration flags
    pa.field('flg_updated_on_hubspot', pa.bool_()),  # 1727/2634
    pa.field('flg_updated_on_iterable', pa.bool_()),  # 2634/2634

    # Legacy and Optional
    pa.field('val_legacy_id', pa.int64()),  # 1038/2634
    pa.field('des_acquisition_form_number', pa.string()),  # 601/2634 have acquisition

    # Dogs data (aggregated from dogs array)
    pa.field('val_dogs_count', pa.int64()),
    pa.field('txt_dog_names', pa.string()),  # comma-separated
    pa.field('val_total_dog_weight', pa.float64()),

    # Comments (aggregated from internalComments array)
    pa.field('val_internal_comments_count', pa.int64()),
    pa.field('ts_last_internal_comment', pa.timestamp('ns')),

    # Standard fields
    pa.field('des_source', pa.string()),
])

# Orders schema - based on actual MongoDB field analysis (19,238 docs)
_orders_schema = pa.schema([
    # Primary Keys & Metadata
    pa.field('pk_order', pa.string()),
    pa.field('pk_yearmonth', pa.string()),
    pa.field('des_data_origin', pa.string()),

    # Order Identification
    pa.field('fk_customer', pa.string()),  # custId
    pa.field('cod_payment', pa.string()),  # payment reference
    pa.field('des_full_name', pa.string()),
    
    # Timestamps
    pa.field('ts_order_created', pa.timestamp('ns')),
    pa.field('ts_order_updated', pa.timestamp('ns')),
    pa.field('ts_delivery_date', pa.timestamp('ns')),
    pa.field('ts_tentative_delivery_date', pa.timestamp('ns')),  # 582/19238
    pa.field('ts_delivery_issue_date', pa.timestamp('ns')),
    pa.field('ts_delivery_issue_added', pa.timestamp('ns')),

    # Order Status & Classification
    pa.field('des_order_status', pa.string()),  # All 19238
    pa.field('des_country', pa.string()),  # All 19238
    
    # Address Information
    pa.field('txt_address_line1', pa.string()),
    pa.field('txt_address_line2', pa.string()),
    pa.field('des_locality', pa.string()),
    pa.field('cod_zip', pa.string()),
    pa.field('des_address_country', pa.string()),
    
    # Contact Info (optional)
    pa.field('des_email', pa.string()),  # 3070/19238
    pa.field('des_phone', pa.string()),  # 3070/19238

    # Order Characteristics - Boolean Flags
    pa.field('flg_is_trial', pa.bool_()),  # 1224/19238
    pa.field('flg_is_secondary', pa.bool_()),  # 5117/19238
    pa.field('flg_is_last_in_cycle', pa.bool_()),  # 11107/19238
    pa.field('flg_is_for_robots', pa.bool_()),  # 4468/19238
    pa.field('flg_is_additional', pa.bool_()),  # 3061/19238
    pa.field('flg_is_first_renewal', pa.bool_()),  # 1000/19238
    pa.field('flg_is_mixed_plan', pa.bool_()),  # 990/19238
    pa.field('flg_is_rescheduled', pa.bool_()),  # 365/19238
    pa.field('flg_is_express_delivery', pa.bool_()),  # 216/19238
    pa.field('flg_has_christmas_extra', pa.bool_()),  # 163/19238
    pa.field('flg_has_additional_ice_bags', pa.bool_()),  # 115/19238
    pa.field('flg_is_agency_pickup', pa.bool_()),  # 115/19238
    pa.field('flg_address_is_locked', pa.bool_()),  # 112/19238
    pa.field('flg_is_legacy', pa.bool_()),  # 10/19238
    pa.field('flg_updated_while_processing', pa.bool_()),  # 12/19238
    pa.field('flg_notification_sent', pa.bool_()),  # 1411/19238

    # Package Details
    pa.field('val_cooling_packs_qty', pa.int64()),  # 11311/19238
    pa.field('val_package_bag_count', pa.int64()),
    pa.field('val_total_package_count', pa.int64()),
    pa.field('val_total_weight_kg', pa.float64()),
    pa.field('val_package_handlers_count', pa.int64()),
    pa.field('flg_package_has_issue', pa.bool_()),
    pa.field('des_package_issue_category', pa.string()),
    pa.field('val_delta_days', pa.int64()),  # 669/19238
    pa.field('des_additional_order_reason', pa.string()),  # 241/19238
    pa.field('des_locked_by', pa.string()),  # 83/19238
    pa.field('val_eligible_promotional_extras_qty', pa.int64()),  # 2/19238

    # Delivery Details
    pa.field('des_delivery_company', pa.string()),
    pa.field('flg_delivery_has_issue', pa.bool_()),
    pa.field('des_delivery_issue_category', pa.string()),

    # Content - Aggregated from bagList
    pa.field('val_total_bags', pa.int64()),
    pa.field('val_chicken_bags', pa.int64()),
    pa.field('val_salmon_bags', pa.int64()),
    pa.field('val_beef_bags', pa.int64()),
    pa.field('val_turkey_bags', pa.int64()),
    pa.field('val_bag_size_100_count', pa.int64()),
    pa.field('val_bag_size_300_count', pa.int64()),
    pa.field('val_bag_size_400_count', pa.int64()),
    pa.field('val_bag_size_500_count', pa.int64()),

    # Standard
    pa.field('des_source', pa.string()),
])

# Payments schema - based on actual MongoDB field analysis (11,215 docs)
_payments_schema = pa.schema([
    # Primary Keys & Metadata
    pa.field('pk_payment', pa.string()),
    pa.field('pk_yearmonth', pa.string()),
    pa.field('des_data_origin', pa.string()),

    # Payment Identification & Linking
    pa.field('fk_customer', pa.string()),  # custId - All 11,215
    pa.field('val_linked_orders_count', pa.int64()),  # count of orders array
    pa.field('txt_linked_order_ids', pa.string()),  # comma-separated order IDs
    
    # Timestamps
    pa.field('ts_payment_date', pa.timestamp('ns')),  # All 11,215
    pa.field('ts_payment_created', pa.timestamp('ns')),  # 11,214/11,215
    pa.field('ts_payment_updated', pa.timestamp('ns')),  # All 11,215
    pa.field('ts_delivery_date', pa.timestamp('ns')),  # 1/11,215

    # Payment Status & Processing
    pa.field('des_payment_status', pa.string()),  # All 11,215
    pa.field('des_country', pa.string()),  # All 11,215
    pa.field('val_failed_attempts_count', pa.int64()),  # 653/11,215
    pa.field('des_error_code', pa.string()),  # 107/11,215

    # Financial Amounts
    pa.field('imp_payment_amount', pa.float64()),  # All 11,215
    pa.field('imp_invoice_amount', pa.float64()),  # All 11,215
    pa.field('imp_discount_amount', pa.float64()),  # All 11,215
    pa.field('pct_discount_percent', pa.float64()),  # All 11,215
    pa.field('imp_extras_amount', pa.float64()),  # 9,961/11,215
    pa.field('imp_shipping_amount', pa.float64()),  # 375/11,215
    pa.field('imp_additional_delivery_amount', pa.float64()),  # 97/11,215
    pa.field('imp_paid_additional_delivery_amount', pa.float64()),  # 7/11,215
    pa.field('imp_delivery_fee', pa.float64()),  # 1/11,215

    # Stripe Integration
    pa.field('fk_stripe_customer', pa.string()),  # 11,214/11,215
    pa.field('cod_stripe_payment_id', pa.string()),  # 10,029/11,215
    pa.field('cod_stripe_charge_id', pa.string()),  # 9,082/11,215
    pa.field('cod_stripe_invoice_id', pa.string()),  # 1/11,215

    # Payment Method
    pa.field('des_payment_method_type', pa.string()),  # 5,272/11,215
    pa.field('cod_card_last4', pa.string()),  # 4,903/11,215

    # Discounts Applied
    pa.field('pct_subscription_discount_applied', pa.float64()),  # from discountsApplied
    pa.field('val_referral_count_applied', pa.int64()),  # from discountsApplied

    # Payment Characteristics - Boolean Flags
    pa.field('flg_is_trial', pa.bool_()),  # 1,113/11,215
    pa.field('flg_is_first_renewal', pa.bool_()),  # 1,005/11,215
    pa.field('flg_is_rescheduled', pa.bool_()),  # 235/11,215
    pa.field('flg_is_additional', pa.bool_()),  # 94/11,215
    pa.field('flg_is_legacy', pa.bool_()),  # 211/11,215
    pa.field('flg_renewal_email_sent', pa.bool_()),  # 7,470/11,215

    # Line Items Aggregation
    pa.field('val_line_items_count', pa.int64()),
    pa.field('val_total_product_qty', pa.int64()),
    pa.field('val_total_product_grams', pa.int64()),
    pa.field('imp_line_items_total_amount', pa.float64()),
    pa.field('txt_products_list', pa.string()),  # comma-separated product names

    # Refunds (rare - only 21 docs)
    pa.field('val_refunds_count', pa.int64()),
    pa.field('imp_total_refund_amount', pa.float64()),
    pa.field('des_latest_refund_status', pa.string()),
    pa.field('des_latest_refund_reason', pa.string()),

    # Optional Fields
    pa.field('cod_pricing_factor', pa.string()),  # 2,067/11,215
    pa.field('cod_coupon', pa.string()),  # 9/11,215
    pa.field('cod_invoice_code', pa.string()),  # 5/11,215

    # Standard
    pa.field('des_source', pa.string()),
])

# Deliveries schema - based on actual MongoDB field analysis (4,171 docs)
_deliveries_schema = pa.schema([
    # Primary Keys & Metadata
    pa.field('pk_delivery', pa.string()),
    pa.field('pk_yearmonth', pa.string()),
    pa.field('des_data_origin', pa.string()),

    # Delivery Identification & Linking
    pa.field('fk_customer', pa.string()),  # custId - All 4,171
    pa.field('des_full_name', pa.string()),  # All 4,171
    pa.field('cod_parcel_id', pa.string()),  # 4,036/4,171
    
    # Timestamps
    pa.field('ts_delivery_created', pa.timestamp('ns')),  # All 4,171
    pa.field('ts_delivery_updated', pa.timestamp('ns')),  # All 4,171
    pa.field('ts_delivery_date_scheduled', pa.timestamp('ns')),  # All 4,171
    pa.field('ts_delivery_date_actual', pa.timestamp('ns')),  # All 4,171 (date field)

    # Delivery Status & Classification
    pa.field('des_delivery_status', pa.string()),  # All 4,171
    pa.field('des_country', pa.string()),  # All 4,171
    pa.field('des_delivery_company', pa.string()),  # All 4,171
    pa.field('des_label_group', pa.string()),  # All 4,171
    
    # Address Information
    pa.field('txt_address_line1', pa.string()),
    pa.field('txt_address_line2', pa.string()),
    pa.field('des_locality', pa.string()),
    pa.field('cod_zip', pa.string()),
    pa.field('des_address_country', pa.string()),

    # Delivery Characteristics - Boolean Flags
    pa.field('flg_is_for_robots', pa.bool_()),  # 1,569/4,171
    pa.field('flg_cust_label_printed', pa.bool_()),  # from isPrinted.cust
    pa.field('flg_internal_label_printed', pa.bool_()),  # from isPrinted.internal

    # Issue Tracking
    pa.field('flg_has_issue', pa.bool_()),  # 183/4,171
    pa.field('txt_issue_reason', pa.string()),  # 183/4,171

    # Label Data (encoded strings - store presence and length for analytics)
    pa.field('flg_has_label_data', pa.bool_()),  # 4,036/4,171
    pa.field('val_label_data_length', pa.int64()),  # length of labelData
    pa.field('flg_has_internal_label_data', pa.bool_()),  # 4,146/4,171
    pa.field('val_internal_label_data_length', pa.int64()),  # length of internalLabelData

    # Standard
    pa.field('des_source', pa.string()),
])

# Coupons schema - based on actual MongoDB field analysis (391 docs)
_coupons_schema = pa.schema([
    # Primary Keys & Metadata
    pa.field('pk_coupon', pa.string()),
    pa.field('pk_yearmonth', pa.string()),
    pa.field('des_data_origin', pa.string()),

    # Coupon Identification
    pa.field('cod_coupon', pa.string()),  # _id field (coupon code)
    
    # Timestamps
    pa.field('ts_coupon_created_at', pa.timestamp('ns')),  # 391/391
    pa.field('ts_updated_at', pa.timestamp('ns')),

    # Coupon Properties
    pa.field('des_coupon_type', pa.string()),  # 391/391 (external, internal, etc.)
    pa.field('des_country', pa.string()),  # 334/391 (~85% coverage)
    
    # Status Flags
    pa.field('flg_is_not_applicable', pa.bool_()),  # 120/391 (~31% have this flag)

    # Standard fields
    pa.field('des_source', pa.string()),
])

# Users-metadata schema - based on actual MongoDB field analysis (3,531 docs)
_users_metadata_schema = pa.schema([
    # Primary Keys & Metadata
    pa.field('pk_user_metadata', pa.string()),
    pa.field('pk_yearmonth', pa.string()),
    pa.field('des_data_origin', pa.string()),

    # User Authentication
    pa.field('fk_customer', pa.string()),  # _id field (customer reference)
    pa.field('txt_password_hash', pa.string()),  # 3531/3531
    pa.field('cod_verification_token', pa.string()),  # 2385/3531 (~67%)
    
    # Timestamps
    pa.field('ts_auth_created_at', pa.timestamp('ns')),  # 3536/3531 (slight overflow)
    pa.field('ts_auth_updated_at', pa.timestamp('ns')),  # 3530/3531
    pa.field('ts_last_session_at', pa.timestamp('ns')),  # 1/3531 (rarely used)

    # System Fields
    pa.field('val_version', pa.int64()),  # __v field, 11/3531

    # Standard fields
    pa.field('des_source', pa.string()),
])

# Leads-archive schema - based on actual MongoDB field analysis (3,134 docs)
_leads_archive_schema = pa.schema([
    # Primary Keys & Metadata
    pa.field('pk_lead_archive', pa.string()),
    pa.field('pk_yearmonth', pa.string()),
    pa.field('des_data_origin', pa.string()),

    # Lead Identification
    pa.field('fk_lead', pa.string()),  # _id field
    
    # Timestamps
    pa.field('ts_lead_created_at', pa.timestamp('ns')),  # 3134/3134
    pa.field('ts_lead_updated_at', pa.timestamp('ns')),  # 3124/3134
    pa.field('ts_updated_at', pa.timestamp('ns')),  # 3134/3134
    pa.field('ts_trial_delivery_default', pa.timestamp('ns')),  # 1132/3134
    pa.field('ts_trial_delivery', pa.timestamp('ns')),  # 190/3134

    # Contact Information
    pa.field('des_email', pa.string()),  # 2839/3134
    pa.field('des_phone', pa.string()),  # 2897/3134
    pa.field('des_given_name', pa.string()),  # 20/3134
    pa.field('des_family_name', pa.string()),  # 13/3134
    pa.field('des_name', pa.string()),  # 8/3134
    pa.field('des_country', pa.string()),  # 3134/3134
    pa.field('cod_zip', pa.string()),  # 111/3134

    # Financial Data
    pa.field('imp_trial_amount', pa.float64()),  # 3134/3134
    pa.field('imp_trial_discount', pa.float64()),  # 453/3134
    pa.field('imp_subscription_amount', pa.float64()),  # 3134/3134
    pa.field('imp_subscription_discount', pa.float64()),  # 415/3134
    pa.field('pct_subscription_discount', pa.float64()),  # 178/3134
    pa.field('val_orders_in_cycle', pa.int64()),  # 2747/3134

    # Marketing & Tracking
    pa.field('val_usage_count', pa.int64()),  # 3134/3134
    pa.field('val_mailing_stage', pa.int64()),  # 1516/3134
    pa.field('cod_coupon', pa.string()),  # 131/3134
    pa.field('cod_campaign_id', pa.string()),  # 26/3134
    pa.field('des_email_deliverability', pa.string()),  # 18/3134

    # Sales Data - aggregated from sales object
    pa.field('des_sales_status', pa.string()),  # 3134/3134
    pa.field('ts_sales_assigned_at', pa.timestamp('ns')),
    pa.field('val_sales_reassignment_count', pa.int64()),
    pa.field('val_sales_comments_count', pa.int64()),

    # Dogs - aggregated from dogs array
    pa.field('val_dogs_count', pa.int64()),  # 3134/3134
    pa.field('txt_dog_names', pa.string()),
    pa.field('val_total_dog_weight', pa.float64()),

    # Flags
    pa.field('flg_mixed_plan', pa.bool_()),  # 408/3134
    pa.field('flg_anonymous', pa.bool_()),  # 206/3134
    pa.field('flg_contact_by_sms', pa.bool_()),  # 25/3134
    pa.field('flg_updated_on_hubspot', pa.bool_()),  # 1959/3134
    pa.field('flg_updated_on_iterable', pa.bool_()),  # 407/3134
    pa.field('flg_trial_info_email_pending', pa.bool_()),  # 6/3134
    pa.field('flg_has_purchase_intent', pa.bool_()),  # 2/3134
    pa.field('flg_has_gift', pa.bool_()),  # 1/3134
    pa.field('flg_notification_sent', pa.bool_()),  # 1/3134
    pa.field('flg_unserved_region', pa.bool_()),  # 5/3134

    # Optional Complex Fields - presence flags
    pa.field('flg_has_acquisition_data', pa.bool_()),  # 2245/3134
    pa.field('flg_has_shared_info', pa.bool_()),  # 2842/3134
    pa.field('flg_has_subscription_data', pa.bool_()),  # 172/3134
    pa.field('flg_has_tracking_data', pa.bool_()),  # 157/3134
    pa.field('flg_has_address', pa.bool_()),  # 13/3134
    pa.field('flg_has_payment_log', pa.bool_()),  # 9/3134
    pa.field('flg_has_appointment_request', pa.bool_()),  # 9/3134

    # System Fields
    pa.field('val_version', pa.int64()),  # __v field

    # Standard fields
    pa.field('des_source', pa.string()),
])

# Contacts-logs schema - based on MongoDB field analysis (261 docs)
_contacts_logs_schema = pa.schema([
    # Primary Keys & Metadata
    pa.field('pk_contact_log', pa.string()),  # 260/261 are strings, 1 objectId
    pa.field('pk_yearmonth', pa.string()),
    pa.field('des_data_origin', pa.string()),

    # Timestamps
    pa.field('ts_created_at', pa.timestamp('ns')),  # 260/261
    pa.field('ts_updated_at', pa.timestamp('ns')),  # 260/261
    
    # Log Aggregations
    pa.field('val_logs_count', pa.int64()),  # Number of log entries
    pa.field('val_total_call_duration', pa.int64()),  # Total duration of all calls
    pa.field('val_call_count', pa.int64()),  # Number of call events
    pa.field('val_email_count', pa.int64()),  # Number of email events
    pa.field('val_sms_count', pa.int64()),  # Number of SMS events
    
    # Last Log Details
    pa.field('ts_last_contact', pa.timestamp('ns')),  # Timestamp of last contact
    pa.field('des_last_contact_type', pa.string()),  # Type of last contact (call, email, sms)
    pa.field('des_last_contact_direction', pa.string()),  # Direction of last contact (inbound, outbound)
    pa.field('des_last_contact_status', pa.string()),  # Status of last contact (completed, hangup, etc.)
    pa.field('val_last_contact_duration', pa.int64()),  # Duration of last contact
    pa.field('fk_last_contact_agent', pa.string()),  # Agent ID for last contact
    
    # Standard
    pa.field('des_source', pa.string()),
])

# Retentions schema - based on MongoDB field analysis (189 docs)
_retentions_schema = pa.schema([
    # Primary Keys & Metadata
    pa.field('pk_retention', pa.string()),  # _id field (189/189)
    pa.field('pk_yearmonth', pa.string()),
    pa.field('des_data_origin', pa.string()),

    # Customer Reference
    pa.field('fk_customer', pa.string()),  # cust field (189/189)
    
    # Timestamps
    pa.field('ts_created_at', pa.timestamp('ns')),  # createdAt (189/189)
    pa.field('ts_updated_at', pa.timestamp('ns')),  # updatedAt (189/189)
    pa.field('ts_assigned_at', pa.timestamp('ns')),  # assignedAt (189/189)
    pa.field('ts_paused_at', pa.timestamp('ns')),  # pausedAt (189/189)
    pa.field('ts_reactivated_at', pa.timestamp('ns')),  # reactivatedAt (39/189)
    pa.field('ts_contacted_at', pa.timestamp('ns')),  # contactedAt (38/189)
    pa.field('ts_appointment_at', pa.timestamp('ns')),  # appointmentAt (7/189)
    
    # Status and Assignment
    pa.field('des_retention_status', pa.string()),  # status (189/189)
    pa.field('val_reassignment_count', pa.int64()),  # reassignmentCount (189/189)
    
    # Contact Channels
    pa.field('val_contact_channels_count', pa.int64()),  # count of contactChannels array (112/189)
    pa.field('txt_contact_channels', pa.string()),  # comma-separated channels
    
    # Pause Reason
    pa.field('des_pause_reason_category', pa.string()),  # from reasonForPause.category (45/189)
    pa.field('des_pause_reason_subcategory', pa.string()),  # from reasonForPause.subcategory
    
    # System and Integration
    pa.field('fk_sys_user', pa.string()),  # sysUser (25/189)
    pa.field('cod_zendesk_ticket', pa.string()),  # zendeskTicketId (1/189)
    
    # Boolean Flags
    pa.field('flg_reactivated_by_agent', pa.bool_()),  # isReactivatedByAgent (39/189)
    pa.field('flg_retention_due_to_agent', pa.bool_()),  # isRetentionDueToAgent (39/189)
    
    # Standard
    pa.field('des_source', pa.string()),
])

# Notifications schema - based on MongoDB field analysis (60 docs)
_notifications_schema = pa.schema([
    # Primary Keys & Metadata
    pa.field('pk_notification', pa.string()),  # _id field (60/60)
    pa.field('pk_yearmonth', pa.string()),
    pa.field('des_data_origin', pa.string()),

    # Recipient Information
    pa.field('fk_recipient', pa.string()),  # recipient (60/60)
    pa.field('des_recipient_model', pa.string()),  # recipientModel (60/60)
    
    # Document Reference
    pa.field('fk_document', pa.string()),  # docId (58/60)
    pa.field('des_document_model', pa.string()),  # docModel (58/60)
    
    # Notification Details
    pa.field('des_notification_type', pa.string()),  # notificationType (60/60)
    pa.field('txt_data', pa.string()),  # data as string representation (2/60)
    
    # Timestamps
    pa.field('ts_created_at', pa.timestamp('ns')),  # createdAt (60/60)
    pa.field('ts_updated_at', pa.timestamp('ns')),  # updatedAt (60/60)
    pa.field('ts_read_at', pa.timestamp('ns')),  # readAt (60/60)
    
    # Flags
    pa.field('flg_is_read', pa.bool_()),  # Derived from readAt
    
    # Standard
    pa.field('des_source', pa.string()),
])

# Appointments schema - based on MongoDB field analysis (6 docs)
_appointments_schema = pa.schema([
    # Primary Keys & Metadata
    pa.field('pk_appointment', pa.string()),  # _id field (6/6)
    pa.field('pk_yearmonth', pa.string()),
    pa.field('des_data_origin', pa.string()),

    # References
    pa.field('fk_sys_user', pa.string()),  # sysUserId (6/6)
    pa.field('fk_lead', pa.string()),  # leadId (6/6)
    
    # Timestamps
    pa.field('ts_created_at', pa.timestamp('ns')),  # createdAt (6/6)
    pa.field('ts_updated_at', pa.timestamp('ns')),  # updatedAt (6/6)
    pa.field('ts_starts_at', pa.timestamp('ns')),  # startsAt (6/6)
    
    # Standard
    pa.field('des_source', pa.string()),
])


SCHEMAS = {
    'customers': _customers_schema,
    'leads': _leads_schema,
    'orders': _orders_schema,
    'payments': _payments_schema,
    'deliveries': _deliveries_schema,
    'coupons': _coupons_schema,
    'users_metadata': _users_metadata_schema,
    'leads_archive': _leads_archive_schema,
    'contacts_logs': _contacts_logs_schema,
    'retentions': _retentions_schema,
    'notifications': _notifications_schema,
    'appointments': _appointments_schema,

}