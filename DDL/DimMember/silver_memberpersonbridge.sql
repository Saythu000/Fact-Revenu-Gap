CREATE TABLE IF NOT EXISTS claimsprocessing.silver.silver_memberpersonbridge (
 ESAIInternalPersonID string
,IsCurrent int
,UniqueRecord string
,FileLayoutID string
,FileId bigint
,LastName string
,FirstName string
,DateOfBirth string
,Gender string  
,PermanentAddressLine1 string 
,PhoneNumber string 
,PlanMemberID string
,BeneficiaryID string
,UniquePersonKey string
,hashKey string
,IsCurrentPlanMemberID int
,IsCurrentUniquePersonKey int
,IsOriginalMemberID int
,PMUP string
,IsCurrentPMUP int
)  USING delta;