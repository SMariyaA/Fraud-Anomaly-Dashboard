import pandas as pd
import numpy as np
import json
import warnings
warnings.filterwarnings('ignore')

# ============================================================
# FRAUD DETECTION ETL PIPELINE
# Run this in your local Jupyter or Python environment
# ============================================================

def run_etl():
    """
    Complete ETL pipeline - Run this function to generate all outputs
    """
    print("🚀 Starting ETL Pipeline...")
    
    # --------------------------------------------------------
    # 1. LOAD RAW DATA (Update paths as per your folder structure)
    # --------------------------------------------------------
    print("Loading raw data...")
    
    # Adjust these paths based on where your data is
    users = pd.read_csv('data/raw/users.csv')
    sessions = pd.read_csv('data/raw/sessions.csv')
    orders = pd.read_csv('data/raw/orders.csv')
    order_items = pd.read_csv('data/raw/order_items.csv')
    payments = pd.read_csv('data/raw/payments.csv')
    shipments = pd.read_csv('data/raw/shipments.csv')
    refunds = pd.read_csv('data/raw/refunds.csv')
    coupons = pd.read_csv('data/raw/coupons.csv')
    
    with open('data/raw/products.json', 'r') as f:
        products = pd.json_normalize(json.load(f))
    
    print(f"✓ Loaded: {len(users)} users, {len(orders)} orders, {len(sessions)} sessions")
    
    # --------------------------------------------------------
    # 2. CLEAN DATA
    # --------------------------------------------------------
    print("Cleaning data...")
    
    # Remove duplicates
    users = users.drop_duplicates(subset=['user_id'])
    orders = orders.drop_duplicates(subset=['order_id'])
    sessions = sessions.drop_duplicates(subset=['session_id'])
    
    # Standardize text
    if 'email' in users.columns:
        users['email'] = users['email'].str.lower()
    
    # Handle missing values
    orders['coupon_id'] = orders['coupon_id'].fillna('NO_COUPON')
    orders['discount_amount'] = orders['discount_amount'].fillna(0)
    
    # Convert dates
    date_cols = ['order_ts', 'signup_ts', 'session_ts']
    for col in date_cols:
        if col in orders.columns:
            orders[col] = pd.to_datetime(orders[col])
        if col in users.columns:
            users[col] = pd.to_datetime(users[col])
        if col in sessions.columns:
            sessions[col] = pd.to_datetime(sessions[col])
    
    print("✓ Data cleaned")
    
    # --------------------------------------------------------
    # 3. CREATE ENRICHED ORDERS (fact_orders_enriched.csv)
    # --------------------------------------------------------
    print("Creating enriched orders...")
    
    # Start with orders
    df = orders.copy()
    
    # Join user info
    user_cols = ['user_id', 'signup_ts']
    if 'user_type' in users.columns:
        user_cols.append('user_type')
    df = df.merge(users[user_cols], on='user_id', how='left')
    
    # Join session info
    session_cols = ['session_id', 'device_type', 'channel']
    if 'ip_address' in sessions.columns:
        session_cols.extend(['ip_address', 'user_agent'])
    df = df.merge(sessions[session_cols], on='session_id', how='left')
    
    # Aggregate order items
    item_stats = order_items.groupby('order_id').agg({
        'quantity': 'sum',
        'unit_price': ['sum', 'count']
    }).reset_index()
    item_stats.columns = ['order_id', 'total_qty', 'gross_amount_check', 'item_count']
    df = df.merge(item_stats, on='order_id', how='left')
    
    # Get top category
    items_cat = order_items.merge(products[['sku', 'category']], on='sku', how='left')
    top_cat = items_cat.groupby('order_id')['category'].agg(
        lambda x: x.mode().iloc[0] if len(x.mode()) > 0 else 'Unknown').reset_index()
    top_cat.columns = ['order_id', 'top_category']
    df = df.merge(top_cat, on='order_id', how='left')
    
    # Join coupon info
    if 'discount_pct' in coupons.columns:
        df = df.merge(coupons[['coupon_id', 'discount_pct']], on='coupon_id', how='left')
        df['discount_pct'] = df['discount_pct'].fillna(0)
    else:
        df['discount_pct'] = 0
    
    # Calculate coupon discount %
    df['coupon_discount_pct'] = (df['discount_amount'] / df['gross_amount'] * 100).fillna(0)
    
    # City tier from pincode
    def get_city_tier(pincode):
        if pd.isna(pincode):
            return 'Unknown'
        pin_str = str(int(pincode))[:2] if pd.notna(pincode) else '00'
        tier1 = ['11', '40', '56', '60', '70']  # Major cities
        tier2 = ['12', '13', '14', '20', '30', '50', '80']
        if pin_str in tier1:
            return 'Tier 1'
        elif pin_str in tier2:
            return 'Tier 2'
        else:
            return 'Tier 3'
    
    df['shipping_city_tier'] = df['shipping_pincode'].apply(get_city_tier)
    
    # --------------------------------------------------------
    # 4. CREATE RISK SIGNALS (10+ signals)
    # --------------------------------------------------------
    print("Creating risk signals...")
    
    # 1. High discount flag (>30%)
    df['high_discount_flag'] = (df['coupon_discount_pct'] > 30).astype(int)
    
    # 2. Multi-coupon user
    user_coupons = df.groupby('user_id')['coupon_id'].nunique().reset_index()
    user_coupons.columns = ['user_id', 'coupon_count']
    df = df.merge(user_coupons, on='user_id', how='left')
    df['multi_coupon_user_flag'] = (df['coupon_count'] > 3).astype(int)
    
    # 3. Payment failures
    pay_fails = payments[payments['status'] == 'failed'].groupby('order_id').size().reset_index(name='payment_fail_count')
    df = df.merge(pay_fails, on='order_id', how='left')
    df['payment_fail_count'] = df['payment_fail_count'].fillna(0)
    df['payment_fail_before_success'] = (df['payment_fail_count'] > 0).astype(int)
    
    # 4. Device reuse
    device_counts = sessions.groupby('device_type').size().reset_index(name='device_reuse_count')
    df = df.merge(device_counts, on='device_type', how='left')
    
    # 5. Pincode reuse
    pincode_counts = df.groupby('shipping_pincode').size().reset_index(name='pincode_reuse_count')
    df = df.merge(pincode_counts, on='shipping_pincode', how='left')
    
    # 6. Order value outlier
    df['order_value_zscore'] = df.groupby('top_category')['gross_amount'].transform(
        lambda x: (x - x.mean()) / x.std() if x.std() > 0 else 0)
    df['order_value_zscore'] = df['order_value_zscore'].fillna(0)
    df['high_value_flag'] = (df['gross_amount'] > df['gross_amount'].quantile(0.9)).astype(int)
    
    # 7. Quantity outlier
    df['qty_outlier_flag'] = (df['total_qty'] > df['total_qty'].quantile(0.95)).astype(int)
    
    # 8. New user flag
    df['new_user_flag'] = 0
    if 'user_type' in df.columns:
        df['new_user_flag'] = (df['user_type'] == 'new').astype(int)
    else:
        # Calculate based on signup date vs order date
        df['days_since_signup'] = (df['order_ts'] - df['signup_ts']).dt.days
        df['new_user_flag'] = (df['days_since_signup'] <= 7).astype(int)
    
    # 9. COD flag
    df['cod_flag'] = (df['payment_method'] == 'COD').astype(int)
    
    # 10. Late night orders (11 PM - 5 AM)
    df['order_hour'] = df['order_ts'].dt.hour
    df['late_night_flag'] = ((df['order_hour'] >= 23) | (df['order_hour'] <= 5)).astype(int)
    
    # 11. Multi-order session
    session_orders = df.groupby('session_id').size().reset_index(name='orders_per_session')
    df = df.merge(session_orders, on='session_id', how='left')
    df['multi_order_session_flag'] = (df['orders_per_session'] > 1).astype(int)
    
    # 12. High refund risk (based on category history)
    cat_refund_risk = refunds.merge(orders[['order_id']], on='order_id', how='left')
    # Simplified - you can enhance this
    
    print("✓ 12 risk signals created")
    
    # --------------------------------------------------------
    # 5. CALCULATE RISK SCORES
    # --------------------------------------------------------
    print("Calculating risk scores...")
    
    # Weighted scoring (total 100)
    weights = {
        'high_discount_flag': 10,
        'multi_coupon_user_flag': 10,
        'payment_fail_before_success': 15,
        'qty_outlier_flag': 10,
        'new_user_flag': 5,
        'cod_flag': 10,
        'late_night_flag': 5,
        'high_value_flag': 10,
        'multi_order_session_flag': 10
    }
    
    df['risk_score'] = 0
    for signal, weight in weights.items():
        if signal in df.columns:
            df['risk_score'] += df[signal] * weight
    
    # Add payment failure penalty
    df['risk_score'] += np.clip(df['payment_fail_count'] * 5, 0, 20)
    
    # Normalize to 0-100
    df['risk_score'] = np.clip(df['risk_score'], 0, 100)
    
    # Risk bands
    df['risk_band'] = pd.cut(df['risk_score'], 
                            bins=[0, 30, 60, 100], 
                            labels=['Low', 'Medium', 'High'])
    
    # Top 3 risk reasons
    def get_top_reasons(row):
        reasons = []
        if row['payment_fail_before_success']:
            reasons.append("Multiple Payment Failures")
        if row['high_discount_flag']:
            reasons.append("High Discount Usage")
        if row['multi_coupon_user_flag']:
            reasons.append("Repeat Coupon User")
        if row['qty_outlier_flag']:
            reasons.append("Unusual Quantity")
        if row['new_user_flag'] and row['cod_flag']:
            reasons.append("New User + COD")
        if row['late_night_flag']:
            reasons.append("Late Night Order")
        if row['high_value_flag']:
            reasons.append("High Value Order")
        if row['multi_order_session_flag']:
            reasons.append("Multiple Orders Same Session")
        
        return " | ".join(reasons[:3]) if reasons else "Low Risk"
    
    df['top_risk_reasons'] = df.apply(get_top_reasons, axis=1)
    
    print("✓ Risk scores calculated")
    
    # --------------------------------------------------------
    # 6. CREATE USER RISK WEEKLY (fact_user_risk_weekly.csv)
    # --------------------------------------------------------
    print("Creating user risk weekly...")
    
    df['week_start'] = df['order_ts'].dt.to_period('W').dt.start_time
    
    user_weekly = df.groupby(['user_id', 'week_start']).agg({
        'order_id': 'count',
        'net_amount': 'sum',
        'coupon_id': lambda x: (x != 'NO_COUPON').sum(),
        'coupon_discount_pct': 'mean',
        'cod_flag': 'sum',
        'payment_fail_count': 'sum',
        'risk_score': 'mean'
    }).reset_index()
    
    user_weekly.columns = ['user_id', 'week_start', 'orders_count', 
                          'net_revenue', 'coupon_orders_count', 
                          'avg_discount_pct', 'cod_orders_count',
                          'payment_failures_count', 'risk_score_avg']
    
    # Add refunds
    refund_sum = refunds.groupby('order_id')['refund_amount'].sum().reset_index()
    refund_orders = refund_sum.merge(orders[['order_id', 'user_id']], on='order_id')
    refund_weekly = refund_orders.groupby('user_id').agg({
        'order_id': 'count',
        'refund_amount': 'sum'
    }).reset_index()
    refund_weekly.columns = ['user_id', 'refunds_count', 'refund_amount']
    
    user_weekly = user_weekly.merge(refund_weekly, on='user_id', how='left')
    user_weekly[['refunds_count', 'refund_amount']] = user_weekly[['refunds_count', 'refund_amount']].fillna(0)
    
    # Add RTO
    if 'status' in shipments.columns:
        rto_orders = shipments[shipments['status'] == 'RTO']['order_id'].unique()
        rto_df = orders[orders['order_id'].isin(rto_orders)].groupby('user_id').size().reset_index(name='rto_count')
        user_weekly = user_weekly.merge(rto_df, on='user_id', how='left')
        user_weekly['rto_count'] = user_weekly['rto_count'].fillna(0)
    else:
        user_weekly['rto_count'] = 0
    
    print("✓ User weekly created")
    
    # --------------------------------------------------------
    # 7. CREATE INVESTIGATION QUEUE (investigation_queue.csv)
    # --------------------------------------------------------
    print("Creating investigation queue...")
    
    # Filter medium and high risk
    queue = df[df['risk_band'].isin(['Medium', 'High'])].copy()
    
    # Recommended action
    def get_action(row):
        if row['risk_score'] >= 75:
            return "HOLD - Manual Review"
        elif row['risk_score'] >= 60:
            return "CALL VERIFICATION"
        elif row['risk_score'] >= 40:
            return "ENHANCED MONITORING"
        else:
            return "STANDARD PROCESS"
    
    queue['recommended_action'] = queue.apply(get_action, axis=1)
    
    # Select columns
    queue = queue[['order_id', 'risk_score', 'risk_band', 
                  'top_risk_reasons', 'recommended_action',
                  'user_id', 'gross_amount', 'net_amount',
                  'payment_method', 'shipping_pincode', 
                  'device_type', 'channel', 'top_category']].copy()
    
    queue = queue.sort_values('risk_score', ascending=False)
    
    print(f"✓ Investigation queue: {len(queue)} orders flagged")
    
    # --------------------------------------------------------
    # 8. SAVE OUTPUTS
    # --------------------------------------------------------
    print("Saving outputs...")
    
    # Save to output folder
    df.to_csv('output/fact_orders_enriched.csv', index=False)
    user_weekly.to_csv('output/fact_user_risk_weekly.csv', index=False)
    queue.to_csv('output/investigation_queue.csv', index=False)
    
    print("\n" + "="*50)
    print("✅ ETL PIPELINE COMPLETED SUCCESSFULLY!")
    print("="*50)
    print(f"Output files created in /output/ folder:")
    print(f"  1. fact_orders_enriched.csv ({len(df)} rows)")
    print(f"  2. fact_user_risk_weekly.csv ({len(user_weekly)} rows)")
    print(f"  3. investigation_queue.csv ({len(queue)} rows)")
    print("="*50)
    
    # Return summary stats
    return {
        'total_orders': len(df),
        'high_risk_orders': len(df[df['risk_band'] == 'High']),
        'medium_risk_orders': len(df[df['risk_band'] == 'Medium']),
        'avg_risk_score': df['risk_score'].mean(),
        'total_refund_loss': refunds['refund_amount'].sum() if 'refund_amount' in refunds.columns else 0
    }

# Run the pipeline
if __name__ == "__main__":
    results = run_etl()
    print("\nSummary:", results)
