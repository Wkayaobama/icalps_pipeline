SQL statements

Generated: 2025-09-17T10:45:07.689834+00:00

- Full object names (tables/views) prefixed as requested:

[CRMICALPS_Copy_20250902_142619].[dbo].[Opportunity]  
[CRMICALPS_Copy_20250902_142619].[dbo].[vOpportunity]  
[CRMICALPS_Copy_20250902_142619].[dbo].[vListOpportunity]  
[CRMICALPS_Copy_20250902_142619].[dbo].[vSearchListOpportunity]  
[CRMICALPS_Copy_20250902_142619].[dbo].[vListCommunicationOpportunity]  
[CRMICALPS_Copy_20250902_142619].[dbo].[OpportunityProgress]  
[CRMICALPS_Copy_20250902_142619].[dbo].[vOpportunityProgress]  
[CRMICALPS_Copy_20250902_142619].[dbo].[Person]  
[CRMICALPS_Copy_20250902_142619].[dbo].[vPersons]  
[CRMICALPS_Copy_20250902_142619].[dbo].[mPersonAllDetails]  
[CRMICALPS_Copy_20250902_142619].[dbo].[Company]  
[CRMICALPS_Copy_20250902_142619].[dbo].[vCompany]

**Extract all the contacts associated with companies and opportunities? using views**

SELECT

    p.Pers_PersonId,

    p.Pers_FirstName,

    p.Pers_LastName,

    p.Pers_EmailAddress,

    c.Comp_CompanyId,

    c.Comp_Name,

    o.Oppo_OpportunityId,

    o.Oppo_Description

FROM [CRMICALPS_Copy_20250902_142619].[dbo].[mPersonAllDetails] p

INNER JOIN [CRMICALPS_Copy_20250902_142619].[dbo].[vCompany] c ON p.Pers_CompanyId = c.Comp_CompanyId

INNER JOIN [CRMICALPS_Copy_20250902_142619].[dbo].[vOpportunity] o ON o.Oppo_PrimaryCompanyId = c.Comp_CompanyId;

  
  

----------------------------

**Extract all the opportunities associated with companies and contact using views**

SELECT

    vO.Oppo_OpportunityId,

    vO.Oppo_Description,

    vC.Comp_CompanyId,

    vC.Comp_Name,

    vP.Pers_PersonId,

    vP.Pers_FirstName,

    vP.Pers_LastName,

    vP.Pers_Email

FROM [CRMICALPS_Copy_20250902_142619].[dbo].[vOpportunity] vO

INNER JOIN [CRMICALPS_Copy_20250902_142619].[dbo].[vCompany] vC ON vO.Oppo_PrimaryCompanyId = vC.Comp_CompanyId

INNER JOIN [CRMICALPS_Copy_20250902_142619].[dbo].[vPersons] vP ON vO.Oppo_PrimaryPersonId = vP.Pers_PersonId;  
  

----------------------------

**Extract all the companies associated with opportunity and contact?using views**

SELECT

    vC.Comp_CompanyId,

    vC.Comp_Name,

    vO.Oppo_OpportunityId,

    vO.Oppo_Description,

    vP.Pers_PersonId,

    vP.Pers_FirstName,

    vP.Pers_LastName,

    vP.Pers_Email

FROM [CRMICALPS_Copy_20250902_142619].[dbo].[vCompany] vC

INNER JOIN [CRMICALPS_Copy_20250902_142619].[dbo].[vOpportunity] vO ON vO.Oppo_PrimaryCompanyId = vC.Comp_CompanyId

INNER JOIN [CRMICALPS_Copy_20250902_142619].[dbo].[vPersons] vP ON vO.Oppo_PrimaryPersonId = vP.Pers_PersonId;  
  

----------------------------

**Extract all the social networks associated with the custom table and their joined id?**

SELECT

    s.sone_networklink,

    s.sone_tableid AS Related_TableID,

    s.sone_recordid AS Related_RecordID,

    ct.bord_caption,

    ct.Bord_DescriptionField,

    ct.Bord_CompanyUpdateFieldName,

    ct.Bord_Component,

    c.Comp_CompanyId,

    c.Comp_Name,

    p.Pers_PersonId,

    p.Pers_FirstName,

    p.Pers_LastName

FROM dbo.MC_socialnetworks s

JOIN dbo.Custom_Tables ct ON s.sone_tableid = ct.Bord_TableId

LEFT JOIN dbo.Company c ON s.sone_recordid = c.Comp_CompanyId

LEFT JOIN dbo.Person p ON s.sone_recordid = p.Pers_PersonId

WHERE s.sone_Deleted IS NULL

----------------------------

**Extract all the communications and their relative associated records (contacts,companies or opportunities)**

SELECT

    vFC.Comm_CommunicationId,

    vFC.Comm_Subject,

    vFC.Comm_From,

    vFC.Comm_TO,

    vFC.Comm_Date,

    vFC.Oppo_OpportunityId,

    vFC.Oppo_Description,

    vFC.Pers_PersonId,

    vFC.Pers_FirstName,

    vFC.Pers_LastName,

    vFC.Comp_CompanyId,

    vFC.Comp_Name

FROM [CRMICALPS_Copy_20250902_142619].[dbo].[vFindCommunication] vFC;

----------------------------

