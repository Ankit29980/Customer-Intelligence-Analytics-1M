
# 📊 E-Commerce Strategic Analytics: 1M+ Transactions Deep-Dive

## 📝 Project Overview
This project delivers a comprehensive analytics solution for a large-scale e-commerce dataset containing over **1 Million transactions**. The goal was to transform raw transactional data into actionable business intelligence, focusing on customer segmentation, retention patterns, and product associations.

---

## 🛠️ Technical Architecture & Data Engineering
* **Scalable Data Modeling:** Re-engineered a monolithic 1M row dataset into a high-performance **Star Schema** (Fact & Dimension tables).
* **Big Data Optimization:** Shifted heavy business logic and aggregations to **SQL Server Views** to ensure a lightweight and responsive Power BI experience.
* **Data Integrity:** Implemented surrogate keys and unique customer mapping to maintain 100% accuracy for **849,546 unique customers**.
* **Performance Tuning:** Optimized DAX measures to allow instantaneous filtering across a million rows.

---

## 📈 Key Analytical Modules
1. **RFM Segmentation:** Categorized customers into 11 distinct loyalty segments (e.g., **Champions**, **At Risk**, **Hibernating**) to drive personalized marketing.
2. **ABC & Customer-Based Association:** Utilized Lift and Support scores to identify high-affinity product pairs (e.g., Smartphones + Headphones) for cross-selling.
3. **Pareto (80/20) Analysis:** Quantified revenue concentration, identifying that **20% of brands** (Apple, Samsung, Xiaomi) generate **80% of total revenue**.
4. **Dynamic Cohort Analysis:** Developed retention heatmaps to track **Net Revenue Retention (NRR)** and identify critical churn points.

---

## 💡 Top Business Insights
* **Core Revenue Driver:** **113.72K Champions** represent the most loyal base, showing a high preference for premium tech bundles.
* **Retention Gap:** Cohort analysis revealed a significant **Month 1 drop-off**, suggesting a need for enhanced post-purchase loyalty programs.
* **Category Dominance:** Electronics and Computers drive **73.32% of total sales**, providing a clear roadmap for inventory and marketing focus.
* **Churn Mitigation:** Identified **112.44K Hibernating** customers as the primary group for urgent "Win-back" re-engagement campaigns.

---

## 📂 Repository Structure
* `/SQL_Scripts`: Optimized Views and Data Pre-processing scripts.
* `README.md`: Detailed project documentation and insights.

---

## 🔗 Live Link
https://app.fabric.microsoft.com/view?r=eyJrIjoiMDllODFiY2EtODM5MC00YjRiLWJhZDItYzg1Y2RmYzdhYTNhIiwidCI6Ijg2MDA1ZGZiLWEyZjEtNDdiYy1hNTVkLTkwYmI1MzI0Y2NjYSJ9
