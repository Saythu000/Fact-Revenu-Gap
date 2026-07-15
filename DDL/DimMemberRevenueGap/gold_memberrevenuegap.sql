CREATE TABLE IF NOT EXISTS claimsprocessing.gold.gold_memberRevenueGap (
   planMemberID         string
  ,hccNumber            string
  ,reportMonth          string
  ,clientCode           string
  ,memberFirstName      string
  ,memberLastName       string
  ,memberDOB            string
  ,hccVersion           string
  ,hccDescription       string
  ,providerID           string
  ,providerNPI          string
  ,providerLastName     string
  ,providerFirstName    string
  ,practiceCode         string
  ,practiceName         string
  ,market               string
  ,alertCategory        string
  ,closureReason        string
  ,lastDCConfirmedDate  date
  ,lastPCPVisitDate     date
  ,lastAWVDate          date
  ,snapshotDate         date
  ,planID               string
  ,hashKey              string
) USING delta;
