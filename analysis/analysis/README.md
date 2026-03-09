# Fraud Analysis & Insights

## Key Performance Indicators (KPIs)

| KPI | Definition | Current Value |
|-----|-----------|---------------|
| High Risk Order Rate | % of orders with risk_score &gt; 60 | ~15% |
| Avg Risk Score | Mean risk score across all orders | 35.2 |
| Investigation Queue | Orders flagged for manual review | 0 (threshold issue) |
| COD Risk Exposure | % of COD orders flagged high risk | ~25% |
| Payment Failure Rate | Orders with failed payment attempts | ~12% |

## Top Insights

1. **High Discount Abuse**: 18% of orders use &gt;30% discount (potential coupon abuse)
2. **New User + COD Pattern**: 23% of new users choose COD (high RTO risk)
3. **Late Night Orders**: 8% of orders between 11PM-5AM (unusual pattern)
4. **Device Reuse**: 45 devices shared across multiple accounts

## Risk Scoring System

| Signal | Weight | Threshold |
|--------|--------|-----------|
| High Discount (&gt;30%) | 10 | Flag if True |
| Multi-Coupon User | 10 | &gt;3 coupons |
| Payment Failures | 5 per fail | &gt;0 fails |
| New User + COD | 15 | Both True |
| Late Night | 5 | 11PM-5AM |
| Qty Outlier | 10 | &gt;95th percentile |

**Risk Bands**:
- Low: 0-25
- Medium: 25-50  
- High: 50-75
- Critical: 75-100

## 30-Day Impact Estimate

| Metric | Estimate |
|--------|----------|
| Fraud Loss Prevention | ₹15-20L |
| Reduced RTO Rate | 12-15% |
| Investigation Efficiency | 40% faster |
| False Positive Rate | ~8% |

## Controls & Experiments

1. **Immediate**: Hold orders with risk_score &gt;75
2. **Experiment**: A/B test stricter coupon limits
3. **Monitor**: Track conversion impact vs fraud reduction
