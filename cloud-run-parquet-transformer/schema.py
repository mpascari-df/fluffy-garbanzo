import pyarrow as pa

# This file defines the target PyArrow schemas for the transformed data
# before it is written to Parquet. Each schema corresponds to a
# specific MongoDB collection.

# Schema for the 'customers' collection.
# It defines the target structure for the data before it's written to Parquet.
_customers_schema = pa.schema([
    # Primary Keys & Metadata
    pa.field('pk_client', pa.string()),
    pa.field('pk_yearmonth', pa.string()),
    pa.field('des_data_origin', pa.string()),

    # Timestamps
    pa.field('ts_customer_created_at', pa.timestamp('ns')),
    pa.field('ts_updated_at', pa.timestamp('ns')),
    pa.field('ts_lead_created_at', pa.timestamp('ns')),
    pa.field('ts_lead_updated_at', pa.timestamp('ns')),
    pa.field('ts_last_session', pa.timestamp('ns')),
    pa.field('ts_sales_assigned', pa.timestamp('ns')),
    pa.field('ts_trial_delivery_default', pa.timestamp('ns')),
    pa.field('ts_trial_delivery', pa.timestamp('ns')),
    pa.field('des_status_updated_at', pa.string()),
    pa.field('ts_initial_payment', pa.timestamp('ns')),
    pa.field('ts_first_mixed_plan', pa.timestamp('ns')),

    # Customer Information
    pa.field('des_email', pa.string()),
    pa.field('des_phone', pa.string()),
    pa.field('des_given_name', pa.string()),
    pa.field('des_family_name', pa.string()),
    pa.field('des_country', pa.string()),
    pa.field('txt_adress', pa.string()),
    pa.field('txt_adress_aux', pa.string()),
    pa.field('des_locality', pa.string()),
    pa.field('cod_zip', pa.string()),
    pa.field('des_country_address', pa.string()),
    pa.field('des_coupon_snapshot', pa.string()),

    # Sales & Acquisition
    pa.field('fk_sales_agent', pa.string()),
    pa.field('des_conversion_source', pa.string()),
    pa.field('val_lead_usage_count', pa.int64()),
    pa.field('des_sales_status', pa.string()),
    pa.field('val_sales_reassigment', pa.int64()),
    pa.field('txt_not_interested_reason', pa.string()),
    pa.field('des_acquisition_source_first', pa.string()),
    pa.field('des_acquisition_source_last', pa.string()),
    pa.field('des_acquisition_medium_first', pa.string()),
    pa.field('des_acquisition_medium_last', pa.string()),
    pa.field('des_acquisition_campaign_first', pa.string()),
    pa.field('des_acquisition_campaign_last', pa.string()),
    pa.field('des_acquisition_content_first', pa.string()),
    pa.field('des_acquisition_content_last', pa.string()),
    pa.field('des_acquisition_term_first', pa.string()),
    pa.field('des_acquisition_term_last', pa.string()),
    pa.field('des_acquisition_awc_first', pa.string()),
    pa.field('des_acquisition_awc_last', pa.string()),
    pa.field('des_acquisition_keyword_first', pa.string()),
    pa.field('des_acquisition_keyword_last', pa.string()),
    pa.field('des_acquisition_affiliate_first', pa.string()),
    pa.field('des_acquisition_affiliate_last', pa.string()),

    # Subscription & Financial
    pa.field('des_subscription_status', pa.string()),
    pa.field('cod_status_updated_by', pa.string()),
    pa.field('des_contacted_after_status_updated', pa.bool_()),
    pa.field('flg_active_orders', pa.bool_()),
    pa.field('flg_active_payments', pa.bool_()),
    pa.field('txt_payment_log', pa.string()),
    pa.field('cod_stripe_initial_payment', pa.string()),
    pa.field('cod_pause_reason', pa.string()),
    pa.field('txt_customer_pause_reason', pa.string()),
    pa.field('pct_come_back_probability', pa.float64()),
    pa.field('val_order_cycle', pa.int64()),
    pa.field('val_payment_cycle_weeks', pa.int64()),
    pa.field('val_daily_grams', pa.int64()),
    pa.field('des_delivery_company', pa.string()),
    pa.field('imp_trial_amount', pa.float64()),
    pa.field('imp_trial_discount', pa.float64()),
    pa.field('imp_subscription', pa.float64()),
    pa.field('imp_subscription_discount', pa.float64()),
    pa.field('imp_subscription_extras', pa.float64()),
    pa.field('imp_paid_orders', pa.float64()),
    pa.field('val_count_paid_orders', pa.int64()),
    pa.field('cod_coupon_applied', pa.string()),
    pa.field('cod_coupon', pa.string()),
    pa.field('val_referral_coupon', pa.int64()),
    pa.field('val_discount_extra_coupon', pa.float64()),
    pa.field('fk_stripe', pa.string()),
    pa.field('cod_payment_method', pa.string()),
    pa.field('des_payment_method', pa.string()),
    pa.field('val_payment_issues', pa.int64()),

    # Flags & Features
    pa.field('flg_additional_ice_bags', pa.bool_()),
    pa.field('flg_express_delivery', pa.bool_()),
    pa.field('flg_mixed_plan', pa.bool_()),
    pa.field('cod_pricing_factor', pa.string()),
    pa.field('flg_apply_der', pa.bool_()),

    # Additional Fields
    pa.field('fk_client_zdk', pa.string()),
    pa.field('fk_group', pa.string()),
    pa.field('fk_organization', pa.string()),
    pa.field('des_source', pa.string()),
])

# A dictionary to hold all schemas, keyed by collection name.
# This allows the pipeline to dynamically select the correct schema.
SCHEMAS = {
    'customers': _customers_schema,
    # Add new collections here:
    # 'leads': _leads_schema,
    # 'orders': _orders_schema,
}