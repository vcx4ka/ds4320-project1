# New Machine Learning Model Predicts Soccer Player Performance Decline with 93.8% Accuracy
## What if coaches could predict which players are about to hit a performance slump before it happens?
A new machine learning model developed by University of Virginia data science researchers can now identify soccer players at risk of performance decline with 93.8% accuracy, potentially revolutionizing how teams manage player workload and training.

## Problem Statement
Professional soccer clubs invest millions in player salaries, yet have limited tools to predict performance fluctuations. When a star player's performance declines, it can cost teams crucial matches, tournament qualifications, and player value. Current approaches rely on subjective coach observations or simple statistics like goals scored, which fail to capture the nuanced patterns that precede a decline. This new project aimed to answer one question: Can we predict whether a player's performance will decrease in their next match using only publicly available match data and player attributes? This ability would give coaching staff a proactive tool to identify at-risk players before performance drops impact team results.

## Solution Description
We developed a machine learning model that analyzes over 10 years of European soccer data (2008-2016) to predict performance decline. The model considers the following factors that contribute to player performance:

- **Recent Form**: Performance trend over the last 3 games (the strongest predictor)
- **Technical Skills**: Rating, finishing, dribbling, and passing abilities
- **Physical Attributes**: Height, weight, and age
- **Match Context**: Home/away advantage, team performance, and goal difference

The system processes data from a collection of players and matches to generate a "decline probability" score for each player before every match. Coaches receive an alert when a player shows signs of impending decline, enabling proactive interventions such as adjusting training intensity, providing additional rest days, rotating squad members strategically, and offering mental health support. The researchers found that a players' recent form was the strongest predictor of decline. A player's performance over their last 3 games was much more important than other features, including those quantifying their technical skills.

## Chart
![Model Performance Comparison](plots/model_comparison_roc_curves.png)

*Figure: ROC curves comparing three machine learning models. The Logistic Regression model  achieves the highest performance with an AUC of 0.938, meaning it correctly distinguishes between players who will decline and those who won't 93.8% of the time.*

## Data Availability
The cleaned dataset (4 tables, 545,538 records) is available on UVA OneDrive: [UVA OneDrive Data Folder](https://myuva-my.sharepoint.com/:f:/g/personal/vcx4ka_virginia_edu/IgDkm3bSO4ddRrfrgJUvnP2FARFKLKoxtnxQnPdcKKCrPbU?e=kzwiJS)
