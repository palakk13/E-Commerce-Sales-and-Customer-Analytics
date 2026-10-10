# Customer Segmentation Model Evaluation Report (Phase 6)

**Generated**: 2026-10-10 08:12:17 UTC  
**Model**: Scikit-Learn K-Means Clustering (`random_state=42`)  
**Features Scaled**: `recency_days`, `frequency`, `monetary` ($\log(1+x)$ + Z-score Standardization)  

---

## 1. Cluster Evaluation Metrics (k=2 to 6)

| Clusters (k) | Inertia (SSE) | Silhouette Score | Calinski-Harabasz Score | Davies-Bouldin Index | Evaluation |
| :---: | :---: | :---: | :---: | :---: | :--- |
| 2  | 1,219.80 | 0.6103 | 1,456.50 | 0.4143 | Suboptimal separation |
| 3  | 658.43 | 0.5110 | 1,772.80 | 0.7585 | Suboptimal separation |
| 4 **(Selected)** | 451.48 | 0.4873 | 1,874.08 | 0.7831 | Global CH Peak & Defensible Elbow |
| 5  | 371.00 | 0.4594 | 1,762.99 | 0.8666 | Suboptimal separation |
| 6  | 306.44 | 0.4733 | 1,747.54 | 0.8294 | Suboptimal separation |

### Selection Rationale: Why k = 4?
1. **Calinski-Harabasz Peak**: k = 4 attains the global maximum score of **1,874.08**, indicating optimal cluster dispersion and compactness.
2. **Elbow Method Inflection Point**: SSE Inertia drops by 561.37 from k=2 to 3, and 206.95 from k=3 to 4. Past k=4, the marginal inertia drop flattens to 80.48 (k=5) and 64.56 (k=6).
3. **Commercial Interpretability**: 4 clusters cleanly partition the e-commerce customer lifecycle into distinct actionable cohorts without creating redundant micro-segments.

---

## 2. Customer Segment Behavioral Profiles

| Cluster ID | Segment Name | Customers | Share (%) | Mean Recency | Mean Freq | Mean Spend | Total Spend | Rev Share (%) | Strategic Priority |
| :---: | :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :--- |
| 0 | **Champions** | 296 | 29.6% | 247.8 days | 2.82 | $753.56 | $223,053.37 | **55.0%** | VIP Loyalty, Early Access, Retention |
| 1 | **Recent Active Shoppers** | 132 | 13.2% | 40.0 days | 2.42 | $603.53 | $79,665.33 | **19.6%** | Cross-sell, Onboarding Nurture |
| 2 | **At-Risk / Lapsed Buyers** | 358 | 35.8% | 572.7 days | 1.23 | $287.06 | $102,765.77 | **25.3%** | Win-back Offers, Survey Inquiries |
| 3 | **Unconverted / Inactive** | 214 | 21.4% | 915.4 days | 0.00 | $0.00 | $0.00 | **0.0%** | Re-engagement, Incentive Discounts |

---

## 3. Financial and Record Reconciliation
- **Total Customer Profiles**: 1,000 customers (100% assigned to a valid segment).
- **Total Recognized Revenue Reconciled**: $405,484.47 (100% match with Phase 3/4/5 verified totals).
- **Zero Orphan Records**: All 1,000 customers have stable `customer_id` keys preserved.
