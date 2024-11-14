CREATE DATABASE tecknoworks;

CREATE TABLE input_data (
    PatientID	VARCHAR(50)
    , DrugName	VARCHAR(200)
    , StartDate	DATE
    , EndDate   DATE
);

COPY Input_data
FROM 'C:\Users\33665\Documents\Tecknoworks Europe\Input_data.csv'
DELIMITER ',' CSV HEADER;


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