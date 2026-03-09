# Fraud/Anomaly Monitoring Dashboard
## Executive Summary & Recommendations

---

### 1. Objective & North Star

**Objective:** Reduce fraud loss while minimizing false positives that hurt legitimate customers.

**North Star Metric:** Monthly fraud loss prevented (₹)

**KPIs Tracked:**
- Refund rate and amount
- RTO rate (COD orders)
- Coupon usage rate
- Payment failure rate
- Suspicious order rate
- Investigation precision

---

### 2. Data Pipeline Summary

**Input:** 9 raw datasets (users, orders, sessions, payments, shipments, refunds, coupons, products)

**Processing:** Python ETL pipeline with pandas
- Data cleaning: duplicates, missing values, standardization
- Feature engineering: 12 risk signals
- Risk scoring: weighted algorithm (0-100 scale)

**Output:** 3 curated tables
- `fact_orders_enriched.csv` - Order-level with risk scores
- `fact_user_risk_weekly.csv` - User behavior trends
- `investigation_queue.csv` - Actionable alerts for ops team

---

### 3. Key Insights

#### Fraud Landscape
- **12%** of orders are high-risk (score &gt;60)
- **25%** are medium-risk (score 31-60)
- Average risk score: **42.3**

#### Top 5 Fraud Patterns

| Pattern | Volume | Loss Proxy | Concentration |
|---------|--------|------------|---------------|
| Payment Failures | 1,120 orders | ₹168,000 | Card, NetBanking |
| Late Night Orders | 950 orders | ₹142,500 | Tier 3 cities |
| High Discount Abuse | 280 orders | ₹84,000 | New users |
| New User + COD | 420 orders | ₹126,000 | Tier 2/3 cities |
| High Value Orders | 175 orders | ₹175,000 | All channels |

#### Highest Risk Segments
1. **New users + COD + Tier 3:** 68% risk score
2. **Late night + Card payment:** 58% risk score
3. **Multiple coupons + High discount:** 72% risk score

---

### 4. Risk Scoring System

**12 Signals** with weights totaling 100 points:
- Payment failures (15 pts)
- Suspicious email (15 pts)
- High discount (10 pts)
- Repeat coupons (10 pts)
- Unusual quantity (10 pts)
- High value (10 pts)
- COD payment (10 pts)
- Multi-order session (10 pts)
- Late night (5 pts)
- New user (5 pts)

**Thresholds:**
- Low: 0-30 (auto-process)
- Medium: 31-60 (monitor)
- High: 61-100 (hold/review)

---

### 5. Investigation Queue

**420 high-risk orders** flagged for immediate action:
- 180 orders: HOLD - Manual review required
- 240 orders: CALL VERIFICATION

**Top reasons:**
1. Multiple payment failures
2. High discount usage
3. New user + COD combination

---

### 6. Recommended Controls

| Priority | Intervention | Timeline | Expected Impact |
|----------|-------------|----------|-----------------|
| 1 | Hold high-risk orders (&gt;70 score) | Week 1-2 | ₹200K/month savings |
| 2 | OTP for COD &gt;₹2,000 | Week 3-5 | 20% RTO reduction |
| 3 | Coupon limits (2/week) | Week 6-7 | 30% abuse reduction |

---

### 7. 30-Day Impact Estimate

**Base Case Scenario:**
- Loss prevented: ₹215,000
- Operational cost: ₹12,600
- **Net savings: ₹202,400**
- **ROI: 437%**

**Investment required:** ₹37,600 (including one-time setup)
**Payback period:** &lt;1 week

---

### 8. Risks & Limitations

| Risk | Likelihood | Mitigation |
|------|------------|------------|
| High false positives | Medium | Start with threshold &gt;75 |
| Conversion drop | Low | Monitor daily, pause if &gt;2% drop |
| Operational capacity | Medium | Hire 2 temp staff initially |
| Data quality issues | Low | Automated validation checks |

---

### 9. Next Steps

**Immediate (Week 1):**
- [ ] Deploy investigation queue to ops team
- [ ] Begin Intervention 1 (high-risk hold)
- [ ] Set up daily monitoring dashboard

**Short-term (Weeks 2-4):**
- [ ] Analyze false positive rate
- [ ] Refine risk thresholds
- [ ] Launch Intervention 2 (COD OTP)

**Medium-term (Month 2-3):**
- [ ] Implement Intervention 3 (coupon limits)
- [ ] Build automated ML model
- [ ] Expand to real-time scoring

---

**Prepared by:** [Your Name]  
**Date:** March 2025  
**Contact:** [Your Email]

---

## Appendix: Dashboard Screenshots

*See `/dashboard/` folder for:*
- Weekly risk trends
- Fraud pattern analysis
- Segment heatmap
- Investigation queue
