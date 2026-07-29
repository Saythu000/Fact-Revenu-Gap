MERGE INTO DestinationTable t
USING (
    SELECT *
    FROM (
        SELECT NULL AS mID, a.*
        FROM tempMemberSQLScript a
        JOIN DestinationTable t 
            ON a.ESAIInternalPersonID = t.ESAIInternalPersonID
           AND a.memberKey <> t.memberKey
           AND t.isCurrent = TRUE
        
        UNION ALL
        
        SELECT a.ESAIInternalPersonID AS mID, a.*
        FROM tempMemberSQLScript a
    )
) s
ON s.mID = t.ESAIInternalPersonID

WHEN MATCHED AND s.memberKey <> t.memberKey AND t.isCurrent = TRUE THEN 
    UPDATE SET    
        t.effectiveEndDate = CURRENT_DATE(),
        t.isCurrent = FALSE

WHEN NOT MATCHED THEN 
    INSERT (
        memberKey,
        ESAIInternalPersonID,
        uniquePersonKey,
        planMemberID,
        subscriberID,
        beneficiaryID,
        lastName,
        firstName,
        middleInitial,
        enrolleeUniqueID,
        dateofBirth,
        deceasedDate,
        gender,
        permanentAddressLine1,
        permanentAddressLine2,
        permanentCity,
        permanentCounty,
        permanentState,
        permanentZipCode,
        mailingAddressLine1,
        mailingAddressLine2,
        mailingCity,
        mailingState,
        mailingZipCode,
        mailingCounty,
        phoneNumber,
        email,
        medicaidID,
        fax,
        raceCode,
        raceDataSource,
        caretakerFirstName,
        caretakerLastName,
        caretakerMiddleInitial,
        ethnicityCode,
        ethnicityDatasource,
        spokenLanguage,
        spokenLanguagesourcecode,
        writtenLanguageCode,
        writtenLanguageSourcecode,
        otherLanguage,
        otherLanguageSourcecode,
        isUSCitizen,
        alternateKey1,
        alternateKey2,
        alternateKey3,
        alternateKey4,
        alternateKey5,
        alternateKey6,
        alternateKey7,
        alternateKey8,
        alternateKey9,
        alternateKey10,
        maskedMemberID,
        enrolleeEducation,
        enrolleeEmployment,
        effectiveStartDate,
        effectiveEndDate,
        isCurrent,
        ProductID
    )
    VALUES (
        s.memberKey,
        s.ESAIInternalPersonID,
        s.uniquePersonKey,
        s.planMemberID,
        s.subscriberID,
        s.beneficiaryID,
        s.lastName,
        s.firstName,
        s.middleInitial,
        s.enrolleeUniqueID,
        s.dateofBirth,
        s.deceasedDate,
        s.gender,
        s.permanentAddressLine1,
        s.permanentAddressLine2,
        s.permanentCity,
        s.permanentCounty,
        s.permanentState,
        s.permanentZipCode,
        s.mailingAddressLine1,
        s.mailingAddressLine2,
        s.mailingCity,
        s.mailingState,
        s.mailingZipCode,
        s.mailingCounty,
        s.phoneNumber,
        s.email,
        s.medicaidID,
        s.fax,
        s.raceCode,
        s.raceDataSource,
        s.caretakerFirstName,
        s.caretakerLastName,
        s.caretakerMiddleInitial,
        s.ethnicityCode,
        s.ethnicityDatasource,
        s.spokenLanguage,
        s.spokenLanguagesourcecode,
        s.writtenLanguageCode,
        s.writtenLanguageSourcecode,
        s.otherLanguage,
        s.otherLanguageSourcecode,
        s.isUSCitizen,
        s.alternateKey1,
        s.alternateKey2,
        s.alternateKey3,
        s.alternateKey4,
        s.alternateKey5,
        s.alternateKey6,
        s.alternateKey7,
        s.alternateKey8,
        s.alternateKey9,
        s.alternateKey10,
        s.maskedMemberID,
        s.enrolleeEducation,
        s.enrolleeEmployment,
        s.effectiveStartDate,
        s.effectiveEndDate,
        s.isCurrent,
        s.ProductID
    );