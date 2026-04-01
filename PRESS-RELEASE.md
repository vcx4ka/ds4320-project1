# New Machine Learning Model Predicts Soccer Player Performance Decline with 93.8% Accuracy

What if coaches could predict which players are about to hit a performance slump before it happens? A new machine learning model developed by University of Virginia data science researchers can now identify soccer players at risk of performance decline with 93.8% accuracy, potentially revolutionizing how teams manage player workload and training.

## Problem Statement
Professional soccer clubs invest millions in player salaries, yet have limited tools to predict performance fluctuations. When a star player's performance declines, it can cost teams crucial matches, tournament qualifications, and player value. Current approaches rely on subjective coach observations or simple statistics like goals scored, which fail to capture the nuanced patterns that precede a decline.

Our specific problem: Can we predict whether a player's performance will decrease in their next match using only publicly available match data and player attributes? This would give coaching staff a proactive tool to identify at-risk players before performance drops impact team results.

## Solution Description
We developed a machine learning model that analyzes over 10 years of European soccer data (2008-2016) to predict performance decline. The model considers:

- **Recent Form**: Performance trend over the last 3 games (the strongest predictor)
- **Technical Skills**: Rating, finishing, dribbling, and passing abilities
- **Physical Attributes**: Height, weight, and age
- **Match Context**: Home/away advantage, team performance, and goal difference

The system processes data from 11,000+ players and 25,000+ matches to generate a "decline probability" score for each player before every match. Coaches receive an alert when a player shows signs of impending decline, enabling proactive interventions such as:
- Adjusting training intensity
- Providing additional rest days
- Rotating squad members strategically
- Offering mental health support

## Chart
![Model Performance Comparison](plots/model_comparison_roc_curves.png)

*Figure: ROC curves comparing three machine learning models. The Logistic Regression model (blue) achieves the highest performance with an AUC of 0.938, meaning it correctly distinguishes between players who will decline and those who won't 93.8% of the time.*

## Key Findings
- **Recent form is the strongest predictor**: A player's performance over their last 3 games is more important than their career average or technical skills
- **Linear models outperform complex ones**: Logistic Regression (AUC 0.938) beats Random Forest (AUC 0.754), suggesting the relationship between attributes and decline is primarily linear
- **Home advantage matters**: Players perform better and decline less frequently when playing at home
- **Age matters too**: Younger players (<23) show higher decline rates than players in their prime (23-28)

## Business Impact
This model enables soccer clubs to:
- **Reduce injury risk**: By identifying fatigue-related decline before injury occurs
- **Optimize lineups**: Bench players likely to underperform
- **Protect player value**: Maintain performance levels for high-value assets

## Technical Implementation
The solution uses:
- **Data Source**: European Soccer Database (Kaggle)
- **Database**: DuckDB for relational queries and feature engineering
- **Models**: Random Forest, Gradient Boosting, and Logistic Regression
- **Evaluation**: 5-fold cross-validation with ROC-AUC as the primary metric
- **Features**: 18 engineered features including rolling averages, career aggregates, and match context

## Data Availability
The cleaned dataset (4 tables, 545,538 records) is available on UVA OneDrive: [UVA OneDrive Data Folder](https://myuva-my.sharepoint.com/:f:/g/personal/vcx4ka_virginia_edu/IgDkm3bSO4ddRrfrgJUvnP2FARFKLKoxtnxQnPdcKKCrPbU?e=kzwiJS)
