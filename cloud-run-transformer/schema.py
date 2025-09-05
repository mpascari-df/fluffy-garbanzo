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
    pa.field('ts_issue_solved_at', pa.timestamp('ns')),  # from issue.solvedAt

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
    pa.field('flg_is_suspended', pa.bool_()),  # isSuspended flag

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
    pa.field('val_comeback_probability', pa.float64()),  # from reasonForPause.comebackProbab
    
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
    pa.field('txt_notes', pa.string()),  # notes field (8.6% coverage)
    
    # Standard
    pa.field('des_source', pa.string()),
])

# ================ NEW COLLECTIONS BELOW ==================

# Changelogs schema - LARGEST collection (6.6M docs)
# Universal audit trail for all entity changes
_changelogs_schema = pa.schema([
    # Primary Keys & Metadata
    pa.field('pk_changelog', pa.string()),  # entity_id being tracked
    pa.field('pk_yearmonth', pa.string()),
    pa.field('des_data_origin', pa.string()),
    
    # Timestamps
    pa.field('ts_created_at', pa.timestamp('ns')),
    pa.field('ts_updated_at', pa.timestamp('ns')),
    
    # Entity tracking
    pa.field('des_entity_type', pa.string()),  # Extracted from _id prefix (ord_, cust_, pay_, etc)
    pa.field('fk_entity_id', pa.string()),  # The entity being tracked
    
    # Change log aggregations
    pa.field('val_total_changes', pa.int64()),  # Total number of changes
    pa.field('val_system_changes', pa.int64()),  # Changes by SYSTEM
    pa.field('val_api_changes', pa.int64()),  # Changes by apikey01
    pa.field('val_user_changes', pa.int64()),  # Changes by human users
    
    # Latest change info
    pa.field('ts_last_change', pa.timestamp('ns')),
    pa.field('des_last_change_actor', pa.string()),
    pa.field('txt_last_change_key', pa.string()),
    
    # Most changed fields (top 3)
    pa.field('txt_top_changed_fields', pa.string()),  # Comma-separated list
    pa.field('val_unique_fields_changed', pa.int64()),  # Count of unique fields
    
    # Standard
    pa.field('des_source', pa.string()),
])

# Orders-archive schema - Historical orders (789K docs)
_orders_archive_schema = pa.schema([
    # Primary Keys & Metadata
    pa.field('pk_order', pa.string()),
    pa.field('pk_yearmonth', pa.string()),
    pa.field('des_data_origin', pa.string()),

    # Order Identification
    pa.field('fk_customer', pa.string()),  # custId
    pa.field('cod_payment', pa.string()),  # payment reference
    pa.field('des_full_name', pa.string()),  # 99.8% coverage
    
    # Timestamps
    pa.field('ts_order_created', pa.timestamp('ns')),
    pa.field('ts_order_updated', pa.timestamp('ns')),
    pa.field('ts_delivery_date', pa.timestamp('ns')),
    pa.field('ts_initial_date', pa.timestamp('ns')),  # Legacy field
    
    # Order Status & Classification
    pa.field('des_order_status', pa.string()),
    pa.field('des_country', pa.string()),
    
    # Address Information
    pa.field('txt_address_line1', pa.string()),
    pa.field('txt_address_line2', pa.string()),
    pa.field('des_locality', pa.string()),
    pa.field('cod_zip', pa.string()),
    pa.field('des_address_country', pa.string()),
    
    # Contact Info
    pa.field('des_email', pa.string()),  # 1.3% coverage
    pa.field('des_phone', pa.string()),  # 1.3% coverage
    
    # Order Characteristics - Boolean Flags
    pa.field('flg_is_trial', pa.bool_()),
    pa.field('flg_is_secondary', pa.bool_()),
    pa.field('flg_is_last_in_cycle', pa.bool_()),
    pa.field('flg_is_additional', pa.bool_()),
    pa.field('flg_is_first_renewal', pa.bool_()),
    pa.field('flg_is_rescheduled', pa.bool_()),
    pa.field('flg_is_express_delivery', pa.bool_()),
    pa.field('flg_has_additional_ice_bags', pa.bool_()),
    pa.field('flg_address_is_locked', pa.bool_()),
    pa.field('flg_is_legacy', pa.bool_()),
    pa.field('flg_was_moved', pa.bool_()),  # Legacy tracking
    pa.field('flg_was_moved_back', pa.bool_()),  # Legacy tracking
    pa.field('flg_was_reset', pa.bool_()),  # Legacy tracking
    pa.field('flg_notification_sent', pa.bool_()),
    
    # Package Details (99% coverage)
    pa.field('val_package_bag_count', pa.int64()),
    pa.field('val_total_package_count', pa.int64()),
    pa.field('val_total_weight_kg', pa.float64()),
    
    # Delivery Details (18% coverage)
    pa.field('des_delivery_company', pa.string()),
    pa.field('flg_delivery_has_issue', pa.bool_()),
    
    # Content - Simplified structure
    pa.field('val_total_bags', pa.int64()),
    pa.field('val_chicken_portions', pa.int64()),
    pa.field('val_turkey_portions', pa.int64()),
    
    # Legacy fields
    pa.field('des_locked_by', pa.string()),  # 0.1% coverage
    pa.field('des_updated_by', pa.string()),  # 0.02% coverage
    pa.field('val_delta_days', pa.int64()),
    
    # System Fields
    pa.field('val_version', pa.int64()),  # __v field
    
    # Standard
    pa.field('des_source', pa.string()),
])

# Payments-archive schema - Historical payments (563K docs)
_payments_archive_schema = pa.schema([
    # Primary Keys & Metadata
    pa.field('pk_payment', pa.string()),
    pa.field('pk_yearmonth', pa.string()),
    pa.field('des_data_origin', pa.string()),

    # Payment Identification & Linking
    pa.field('fk_customer', pa.string()),  # custId
    pa.field('val_linked_orders_count', pa.int64()),
    pa.field('txt_linked_order_ids', pa.string()),
    
    # Timestamps
    pa.field('ts_payment_date', pa.timestamp('ns')),
    pa.field('ts_payment_created', pa.timestamp('ns')),
    pa.field('ts_payment_updated', pa.timestamp('ns')),
    pa.field('ts_initial_date', pa.timestamp('ns')),  # Legacy field
    
    # Payment Status & Processing
    pa.field('des_payment_status', pa.string()),
    pa.field('des_country', pa.string()),
    pa.field('val_failed_attempts_count', pa.int64()),
    pa.field('flg_first_attempt_failed', pa.bool_()),  # Legacy field
    
    # Financial Amounts
    pa.field('imp_payment_amount', pa.float64()),
    pa.field('imp_invoice_amount', pa.float64()),
    pa.field('imp_discount_amount', pa.float64()),
    pa.field('pct_discount_percent', pa.float64()),
    pa.field('imp_extras_amount', pa.float64()),
    pa.field('imp_shipping_amount', pa.float64()),
    
    # Stripe Integration (evolving coverage)
    pa.field('fk_stripe_customer', pa.string()),  # 99.99%
    pa.field('cod_stripe_payment_id', pa.string()),  # 63%
    pa.field('cod_stripe_invoice_id', pa.string()),  # 23%
    pa.field('cod_stripe_charge_id', pa.string()),  # 21%
    
    # Line Items (39% coverage - growing)
    pa.field('val_line_items_count', pa.int64()),
    pa.field('val_total_product_qty', pa.int64()),
    pa.field('val_total_product_grams', pa.int64()),
    pa.field('imp_line_items_total_amount', pa.float64()),
    
    # Payment Characteristics
    pa.field('flg_is_trial', pa.bool_()),
    pa.field('flg_is_first_renewal', pa.bool_()),
    pa.field('flg_is_additional', pa.bool_()),
    pa.field('flg_is_legacy', pa.bool_()),
    pa.field('flg_was_moved', pa.bool_()),  # Legacy tracking
    
    # Optional Fields
    pa.field('cod_coupon', pa.string()),  # 0.03%
    pa.field('cod_invoice_code', pa.string()),  # 0.001%
    
    # System Fields
    pa.field('val_version', pa.int64()),  # __v field
    
    # Standard
    pa.field('des_source', pa.string()),
])

# Packages schema - Package preparation tracking (3,281 docs)
_packages_schema = pa.schema([
    # Primary Keys & Metadata
    pa.field('pk_package', pa.string()),  # Sequential package ID like "3-005"
    pa.field('pk_yearmonth', pa.string()),
    pa.field('des_data_origin', pa.string()),
    
    # Package Identification
    pa.field('fk_handler', pa.string()),  # Handler/staff member ID
    
    # Timestamps
    pa.field('ts_created_at', pa.timestamp('ns')),
    pa.field('ts_updated_at', pa.timestamp('ns')),
    pa.field('ts_used_at', pa.timestamp('ns')),  # 95% coverage
    
    # Package Configuration
    pa.field('val_bag_count', pa.int64()),  # Number of bags
    pa.field('val_daily_grams', pa.int64()),  # Daily gram allocation
    pa.field('flg_is_trial', pa.bool_()),  # 97% coverage
    pa.field('flg_is_used', pa.bool_()),  # Derived from usedAt
    
    # Standard
    pa.field('des_source', pa.string()),
])

# Engagement-histories schema - Customer engagement tracking (70K docs)
_engagement_histories_schema = pa.schema([
    # Primary Keys & Metadata
    pa.field('pk_engagement', pa.string()),
    pa.field('pk_yearmonth', pa.string()),
    pa.field('des_data_origin', pa.string()),
    
    # Timestamps
    pa.field('ts_created_at', pa.timestamp('ns')),
    pa.field('ts_updated_at', pa.timestamp('ns')),
    
    # Customer Engagement
    pa.field('flg_survey_engaged', pa.bool_()),  # from cust.tests.customerDataSurvey.isEngaged
    pa.field('des_survey_value', pa.string()),  # A/B test value
    
    # Recommendation Data (often empty arrays)
    pa.field('flg_has_daily_grams_recommendations', pa.bool_()),
    pa.field('flg_has_menu_recommendations', pa.bool_()),
    
    # Standard
    pa.field('des_source', pa.string()),
])

# Geocontext schema - IP geolocation mapping (89K docs)
_geocontext_schema = pa.schema([
    # Primary Keys & Metadata
    pa.field('pk_geocontext', pa.string()),  # Date-based sequential ID
    pa.field('pk_yearmonth', pa.string()),
    pa.field('des_data_origin', pa.string()),
    
    # IP Range Data
    pa.field('val_ip_range_start', pa.string()),  # fromIp numeric string
    pa.field('val_ip_range_end', pa.string()),  # toIp numeric string
    pa.field('des_country', pa.string()),  # Country code
    
    # Standard
    pa.field('des_source', pa.string()),
])

# Invalid-phones schema - Phone blacklist (8K docs)
_invalid_phones_schema = pa.schema([
    # Primary Keys & Metadata
    pa.field('pk_phone', pa.string()),  # Phone number as ID
    pa.field('pk_yearmonth', pa.string()),
    pa.field('des_data_origin', pa.string()),
    
    # Phone Data
    pa.field('des_phone_number', pa.string()),  # The invalid phone
    pa.field('des_country', pa.string()),  # Country code
    pa.field('des_created_by', pa.string()),  # SYSTEM
    
    # Timestamps
    pa.field('ts_created_at', pa.timestamp('ns')),
    
    # Standard
    pa.field('des_source', pa.string()),
])

# Sysusers schema - System users (611 docs)
_sysusers_schema = pa.schema([
    # Primary Keys & Metadata
    pa.field('pk_sysuser', pa.string()),  # System user ID
    pa.field('pk_yearmonth', pa.string()),
    pa.field('des_data_origin', pa.string()),
    
    # User Information
    pa.field('des_given_name', pa.string()),
    pa.field('des_family_name', pa.string()),
    pa.field('des_email', pa.string()),
    pa.field('des_country', pa.string()),
    
    # Timestamps
    pa.field('ts_created_at', pa.timestamp('ns')),  # 99.8% coverage
    pa.field('ts_updated_at', pa.timestamp('ns')),  # 99.7% coverage
    pa.field('ts_last_session_at', pa.timestamp('ns')),  # 94.8% coverage
    
    # Roles and Permissions
    pa.field('txt_roles', pa.string()),  # Comma-separated roles array
    pa.field('des_role', pa.string()),  # Single role field (7% - legacy)
    pa.field('txt_manages_countries', pa.string()),  # Comma-separated countries
    
    # Status and Features
    pa.field('flg_is_suspended', pa.bool_()),  # 68.9% coverage
    pa.field('flg_is_sales_available', pa.bool_()),  # from sales.isAvailable
    pa.field('flg_has_sales_tracking', pa.bool_()),  # 45.3% have sales object
    pa.field('flg_has_retentions', pa.bool_()),  # 5.4% have retentions
    
    # Standard
    pa.field('des_source', pa.string()),
])

# Stats schema - Business intelligence data (4,314 docs)
_stats_schema = pa.schema([
    # Primary Keys & Metadata
    pa.field('pk_stat', pa.string()),  # Stat record ID
    pa.field('pk_yearmonth', pa.string()),
    pa.field('des_data_origin', pa.string()),
    
    # Timestamps
    pa.field('ts_created_at', pa.timestamp('ns')),
    pa.field('ts_updated_at', pa.timestamp('ns')),
    
    # Time Period
    pa.field('val_year', pa.int64()),  # 99% coverage
    pa.field('val_month', pa.int64()),  # 99% coverage
    
    # Stat Type (extracted from _id pattern)
    pa.field('des_stat_type', pa.string()),  # SALES-STATS, PICKING-STATS, etc
    pa.field('fk_agent_or_handler', pa.string()),  # Agent or handler ID if applicable
    pa.field('des_stat_country', pa.string()),  # Country if specified
    
    # Aggregated Metrics (simplified from complex nested structures)
    pa.field('val_total_assigned_leads', pa.int64()),
    pa.field('val_total_appointments', pa.int64()),
    pa.field('val_total_sales', pa.int64()),
    pa.field('val_total_not_answered', pa.int64()),
    pa.field('val_total_not_interested', pa.int64()),
    pa.field('val_total_orders', pa.int64()),
    pa.field('val_total_packages', pa.int64()),
    
    # Performance Metrics
    pa.field('pct_conversion_rate', pa.float64()),
    pa.field('pct_retention_rate', pa.float64()),
    pa.field('imp_average_sales', pa.float64()),
    
    # Standard
    pa.field('des_source', pa.string()),
])

# Sysinfo schema - System configuration (7 docs)
_sysinfo_schema = pa.schema([
    # Primary Keys & Metadata
    pa.field('pk_config', pa.string()),  # Configuration type ID
    pa.field('pk_yearmonth', pa.string()),
    pa.field('des_data_origin', pa.string()),
    
    # Timestamps
    pa.field('ts_updated_at', pa.timestamp('ns')),
    
    # Configuration Type
    pa.field('des_config_type', pa.string()),  # CONFIG, STOCKS, AVAILABLE-AGENTS, etc
    
    # Simplified representation of complex config
    pa.field('flg_has_cron_settings', pa.bool_()),
    pa.field('flg_has_email_settings', pa.bool_()),
    pa.field('flg_has_sales_settings', pa.bool_()),
    pa.field('flg_has_robot_settings', pa.bool_()),
    pa.field('flg_has_stock_levels', pa.bool_()),
    pa.field('flg_has_agent_lists', pa.bool_()),
    
    # Stock levels (when type is STOCKS)
    pa.field('val_chicken_stock', pa.int64()),
    pa.field('val_salmon_stock', pa.int64()),
    pa.field('val_beef_stock', pa.int64()),
    pa.field('val_turkey_stock', pa.int64()),
    
    # Standard
    pa.field('des_source', pa.string()),
])

# FIX 2: Define the SCHEMAS dictionary with corrected collection names (underscore → dash)
SCHEMAS = {
    # Existing implementations
    'customers': _customers_schema,
    'leads': _leads_schema,
    'orders': _orders_schema,
    'payments': _payments_schema,
    'deliveries': _deliveries_schema,
    'coupons': _coupons_schema,
    'users-metadata': _users_metadata_schema,  # FIX 2: Changed from users_metadata
    'leads-archive': _leads_archive_schema,  # FIX 2: Changed from leads_archive
    'contacts-logs': _contacts_logs_schema,  # FIX 2: Changed from contacts_logs
    'retentions': _retentions_schema,
    'notifications': _notifications_schema,
    'appointments': _appointments_schema,
    
    # New implementations
    'changelogs': _changelogs_schema,
    'orders-archive': _orders_archive_schema,  # FIX 2: Changed from orders_archive
    'payments-archive': _payments_archive_schema,  # FIX 2: Changed from payments_archive
    'packages': _packages_schema,
    'engagement-histories': _engagement_histories_schema,  # FIX 2: Changed from engagement_histories
    'geocontext': _geocontext_schema,
    'invalid-phones': _invalid_phones_schema,  # FIX 2: Changed from invalid_phones
    'sysusers': _sysusers_schema,
    'stats': _stats_schema,
    'sysinfo': _sysinfo_schema,
}