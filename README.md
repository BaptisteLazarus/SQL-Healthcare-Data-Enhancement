# Introduction
 In this project, I demonstrate my SQL skills by solving a complex healthcare 🏥 data transformation problem. The task involves analyzing pharmacy records 💊 to identify the treatments patients are following, whether they are on monotherapy (a single drug) or combined therapies (multiple drugs). The goal is to transform the raw data into a user-friendly format that compactly represents each patient's treatment periods without overlapping dates. 👩‍⚕️

 🔍 SQL queries? Check them out here: [https://github.com/BaptisteLazarus/SQL-Therapy-Period-Calculation/blob/main/HealthcareIndustry_scriptV2.sql]

 # Background
 ### Context
The dataset originates from the healthcare industry and contains the pharmacy history of patients over the past five years. Each record includes:
- PatientID: A unique identifier for each patient.
- DrugName: The name of the drug prescribed.
- StartDate: The date when the patient collected the drug from the pharmacy.
- EndDate: The expected date until which the drug supply lasts.
### Problem
The goal is to compute the therapies (monotherapies or combined therapies) each patient is following, along with the periods for which they are valid. Specifically:
1. Treatments must be compacted to minimize the number of rows in the result.
2. The treatment periods for a patient should not overlap.
3. The result should indicate the start and end dates of each treatment and the combination of drugs taken during that time.

# Tools Used
- **SQL**: allowing me to query the database
- Database Management System (DBMS): **PostgreSQL**
- Visual Studio Code: my go-to for database management and executing SQL queries.

# The Analysis
Below is a breakdown of the SQL transformations and logic applied:

### 1. Identify Continuous Periods for Each Drug and combine them into Treatment Intervals
```sql
-- We want to check wheter there are continuous periods of medication for each drug for each patient 
WITH partitioned_data AS (
    SELECT *
        -- put the data in chronological order for each drug for each patient 
        , LAG(EndDate) over (partition by PatientID, DrugName order by StartDate) as PreviousEndDate
    FROM input_data
    ORDER BY 
        PatientID
        , DrugName
        , StartDate
)

, compare_periods AS (
    SELECT *
        -- if the following period start more than a day after the end of the previous period
            -- it means that this is a new period
        , case when PreviousEndDate is null or StartDate > (PreviousEndDate + INTERVAL '1 day')
            then 1
                else 0
        end as NewPeriod
    FROM partitioned_data
)

, grouped_periods_prep AS (
    SELECT *
        -- we mark the lines that could be group in a same period
        , SUM(NewPeriod) over (partition by PatientID, DrugName order by StartDate, EndDate) as TreatmentPeriod
    FROM compare_periods
)

-- We group periods by patient and drug
, grouped_periods AS (
    SELECT 
        PatientID
        , DrugName
        , min(StartDate) as StartDate
        , max(EndDate) as EndDate
    FROM grouped_periods_prep
    GROUP BY 
        PatientID
        , DrugName
        , TreatmentPeriod
    ORDER BY 
        PatientID
        , DrugName
        , StartDate
)
```

### 2. Identify all distinct periods during which a patient is medicated to then generate a list of drugs active during each period, creating combined therapies.
```sql
, all_dates AS (
    SELECT
        PatientID
        , StartDate as TreatmentDate
        , 'StartDate' as DateType
    FROM grouped_periods
    UNION ALL
    SELECT
        PatientID
        , EndDate as TreatmentDate
        , 'EndDate' as DateType
    FROM grouped_periods
    ORDER BY
        PatientID
        , TreatmentDate
)

, orderedDates_prep AS (
    SELECT
        PatientID
        , TreatmentDate
        , DateType
        , LEAD(TreatmentDate) over (partition by PatientID order by TreatmentDate) as NextTreatmentDate
        , LEAD(DateType) over (partition by PatientID order by TreatmentDate) as NextDateType
    FROM all_dates
)

, OrderedDates AS (
    SELECT 
        PatientID
        , case when DateType = 'EndDate' 
            then TreatmentDate + INTERVAL '1 day'
                else TreatmentDate 
        end as StartDate
        , case when NextDateType = 'StartDate' 
            then NextTreatmentDate - INTERVAL '1 day'
                else NextTreatmentDate 
        end as EndDate
    FROM OrderedDates_prep
    WHERE NextTreatmentDate is not null
)
-- Above, we have created a table 'OrderedDates' in which we have all the different periods during which each patient has been medicated

-- Join with grouped_periods table to get the list of drugs for each period
, Treatment_period AS (
    SELECT 
        o.PatientID
        , array_to_string(array_agg(distinct g.DrugName order by g.DrugName), '+') as Treatment
        , DATE(o.StartDate) as StartDate
        , DATE(o.EndDate) as EndDate
    FROM OrderedDates o
        INNER JOIN grouped_periods g on g.PatientID = o.PatientID
                                    and g.StartDate <= o.EndDate
                                    and g.EndDate >= o.StartDate
    GROUP BY 
        o.PatientID
        , o.StartDate
        , o.EndDate
)
```

### 3. Repeat the first step for the continuous intervals of several medications

```sql
-- V2: We now need to repeat the exercise for the continuous intervals of several medications

, partitioned_data2 AS (
    SELECT *
        , LAG(EndDate) over (partition by PatientID, Treatment order by StartDate) as PreviousEndDate
    FROM Treatment_period
    ORDER BY 
        PatientID
        , Treatment
        , StartDate
)

, compare_periods2 AS (
    SELECT *

        , case when PreviousEndDate is null or StartDate > (PreviousEndDate + INTERVAL '1 day')
            then 1
                else 0
        end as NewPeriod
    FROM partitioned_data2
)

, grouped_periods_prep2 AS (
    SELECT *
        , SUM(NewPeriod) over (partition by PatientID, Treatment order by StartDate, EndDate) as TreatmentPeriod
    FROM compare_periods2
)

, grouped_periods2 AS (
    SELECT 
        PatientID
        , Treatment
        , min(StartDate) as StartDate
        , max(EndDate) as EndDate
    FROM grouped_periods_prep2
    GROUP BY 
        PatientID
        , Treatment
        , TreatmentPeriod
    ORDER BY 
        PatientID
        , Treatment
        , StartDate
)
```
### 4. Generate final output
```sql
-- Final result
    SELECT 
        PatientID as Patient
        , Treatment
        , StartDate
        , EndDate
    FROM 
        grouped_periods2
    ORDER BY 
        PatientID
        , StartDate
        , Treatment
    ;
```

# Results
The resulting table provides a clear summary of each patient’s treatments, including monotherapies and combined therapies, with their corresponding start and end dates.

| PatientID | DrugName   | StartDate  | EndDate    |
|-----------|------------|------------|------------|
| 1234      | DrugA      | 2021-03-01 | 2021-03-24 |
| 1234      | DrugB      | 2021-03-25 | 2021-03-31 |
| 1234      | DrugB      | 2021-04-01 | 2021-04-15 |
| 5678      | DrugC      | 2021-02-01 | 2021-02-28 |
