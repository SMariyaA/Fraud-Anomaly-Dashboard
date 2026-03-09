# Fraud Control Experiments

## Intervention 1: Hold High-Risk Orders

| Element | Details |
|---------|---------|
| **Problem** | High-risk orders (score &gt;70) shipping without review |
| **Control** | Current auto-processing |
| **Treatment** | Hold orders with risk_score ≥70 for manual review |
| **Primary Metric** | Fraud loss prevented (₹) |
| **Guardrail Metric** | Conversion rate (must not drop &gt;2%) |
| **Duration** | 30 days |
| **Success Threshold** | Prevent ₹50,000+ loss with &lt;5% false positive rate |

---

## Intervention 2: OTP Verification for COD

| Element | Details |
|---------|---------|
| **Problem** | High RTO rate on COD orders |
| **Control** | Current COD flow (no verification) |
| **Treatment** | Mandatory OTP for COD orders &gt;₹2,000 |
| **Primary Metric** | RTO rate reduction |
| **Guardrail Metric** | COD conversion rate |
| **Duration** | 21 days |
| **Success Threshold** | 20% reduction in RTO rate |

---

## Intervention 3: Coupon Usage Limits

| Element | Details |
|---------|---------|
| **Problem** | Coupon abuse by repeat users |
| **Control** | Unlimited coupon usage |
| **Treatment** | Max 2 coupons per user per week |
| **Primary Metric** | Discount abuse reduction |
| **Guardrail Metric** | Overall coupon usage (legitimate users) |
| **Duration** | 14 days |
| **Success Threshold** | 30% reduction in multi-coupon usage |

---

## Implementation Priority

1. **Week 1-2:** Intervention 1 (High-risk hold) - Highest impact
2. **Week 3-5:** Intervention 2 (COD OTP) - Medium impact
3. **Week 6-7:** Intervention 3 (Coupon limits) - Lower impact
