/*
  COUNT(*) for every active CT table for client 15347
  (common_tables minus client_overrides/15347.json ignore).
*/
USE [iPC_2025_Dev7_15347];
GO

SET NOCOUNT ON;

SELECT N'AssetClassOverrideImportData' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[AssetClassOverrideImportData]
UNION ALL
SELECT N'AtRiskInput_Snapshot' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[AtRiskInput_Snapshot]
UNION ALL
SELECT N'AtRiskPackage' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[AtRiskPackage]
UNION ALL
SELECT N'BasisOverrideImportData' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[BasisOverrideImportData]
UNION ALL
SELECT N'BookEffective_Snapshot' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[BookEffective_Snapshot]
UNION ALL
SELECT N'BookK1AdjustmentbyPartner' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[BookK1AdjustmentbyPartner]
UNION ALL
SELECT N'BoxJKLInput_Snapshot' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[BoxJKLInput_Snapshot]
UNION ALL
SELECT N'BoxJKLLineItem' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[BoxJKLLineItem]
UNION ALL
SELECT N'CYAdjustmentInput_Snapshot' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[CYAdjustmentInput_Snapshot]
UNION ALL
SELECT N'CYAdjustmentLineItem' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[CYAdjustmentLineItem]
UNION ALL
SELECT N'ContributionLineWithAttributes' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[ContributionLineWithAttributes]
UNION ALL
SELECT N'CostPercentageWACPeriod_Snapshot' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[CostPercentageWACPeriod_Snapshot]
UNION ALL
SELECT N'CostPercentage_704c_Snapshot' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[CostPercentage_704c_Snapshot]
UNION ALL
SELECT N'CostPercentage_Snapshot' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[CostPercentage_Snapshot]
UNION ALL
SELECT N'CustomFootnoteInput' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[CustomFootnoteInput]
UNION ALL
SELECT N'CustomFootnoteLineItem' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[CustomFootnoteLineItem]
UNION ALL
SELECT N'CustomFootnotePackage' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[CustomFootnotePackage]
UNION ALL
SELECT N'CustomImportData' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[CustomImportData]
UNION ALL
SELECT N'CustomImportData_Snapshot' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[CustomImportData_Snapshot]
UNION ALL
SELECT N'CustomImportDetail' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[CustomImportDetail]
UNION ALL
SELECT N'CustomRegisterDetail' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[CustomRegisterDetail]
UNION ALL
SELECT N'CustomRegisterTemplateDetails' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[CustomRegisterTemplateDetails]
UNION ALL
SELECT N'DF_EntityFeedTransactionMapping' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[DF_EntityFeedTransactionMapping]
UNION ALL
SELECT N'DF_FinancialAlloc_Archive' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[DF_FinancialAlloc_Archive]
UNION ALL
SELECT N'DF_PCAP_Archive' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[DF_PCAP_Archive]
UNION ALL
SELECT N'DefaultAllocationRuleSetup' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[DefaultAllocationRuleSetup]
UNION ALL
SELECT N'ECIRate' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[ECIRate]
UNION ALL
SELECT N'ENU_704cAllocationLogic' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[ENU_704cAllocationLogic]
UNION ALL
SELECT N'ENU_AdjustmentType' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[ENU_AdjustmentType]
UNION ALL
SELECT N'ENU_AllocationBy' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[ENU_AllocationBy]
UNION ALL
SELECT N'ENU_AllocationLogic' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[ENU_AllocationLogic]
UNION ALL
SELECT N'ENU_AllocationPercentageType' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[ENU_AllocationPercentageType]
UNION ALL
SELECT N'ENU_AssetClass' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[ENU_AssetClass]
UNION ALL
SELECT N'ENU_AttributeType' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[ENU_AttributeType]
UNION ALL
SELECT N'ENU_CountryListImports' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[ENU_CountryListImports]
UNION ALL
SELECT N'ENU_CustomAllocations' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[ENU_CustomAllocations]
UNION ALL
SELECT N'ENU_DF_DataList' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[ENU_DF_DataList]
UNION ALL
SELECT N'ENU_DataList' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[ENU_DataList]
UNION ALL
SELECT N'ENU_EntityType' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[ENU_EntityType]
UNION ALL
SELECT N'ENU_Event' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[ENU_Event]
UNION ALL
SELECT N'ENU_FilingStatus' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[ENU_FilingStatus]
UNION ALL
SELECT N'ENU_ForeignCorptypeofPFIC' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[ENU_ForeignCorptypeofPFIC]
UNION ALL
SELECT N'ENU_GlobalMenuGroup' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[ENU_GlobalMenuGroup]
UNION ALL
SELECT N'ENU_IncomeAttributeType' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[ENU_IncomeAttributeType]
UNION ALL
SELECT N'ENU_InvestmentType' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[ENU_InvestmentType]
UNION ALL
SELECT N'ENU_K1Status' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[ENU_K1Status]
UNION ALL
SELECT N'ENU_K3Attribute' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[ENU_K3Attribute]
UNION ALL
SELECT N'ENU_LevelType' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[ENU_LevelType]
UNION ALL
SELECT N'ENU_LineType' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[ENU_LineType]
UNION ALL
SELECT N'ENU_MappingSource' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[ENU_MappingSource]
UNION ALL
SELECT N'ENU_PFICAddressType' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[ENU_PFICAddressType]
UNION ALL
SELECT N'ENU_PFICForeignCorp' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[ENU_PFICForeignCorp]
UNION ALL
SELECT N'ENU_ProcessEntityConfigurations' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[ENU_ProcessEntityConfigurations]
UNION ALL
SELECT N'ENU_ProcessEntityConfigurationsType' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[ENU_ProcessEntityConfigurationsType]
UNION ALL
SELECT N'ENU_RuleGroup' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[ENU_RuleGroup]
UNION ALL
SELECT N'ENU_RuleType' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[ENU_RuleType]
UNION ALL
SELECT N'ENU_SICCodes' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[ENU_SICCodes]
UNION ALL
SELECT N'ENU_StateDataList' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[ENU_StateDataList]
UNION ALL
SELECT N'ENU_TaxClass' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[ENU_TaxClass]
UNION ALL
SELECT N'ENU_TrialBalanceCategory' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[ENU_TrialBalanceCategory]
UNION ALL
SELECT N'ENU_TrialBalanceSource' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[ENU_TrialBalanceSource]
UNION ALL
SELECT N'ENU_UnderlyingType' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[ENU_UnderlyingType]
UNION ALL
SELECT N'Entity' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[Entity]
UNION ALL
SELECT N'EntityAllocationRule_Snapshot' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[EntityAllocationRule_Snapshot]
UNION ALL
SELECT N'EntityConfigurationImportData' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[EntityConfigurationImportData]
UNION ALL
SELECT N'EntityConfigurations' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[EntityConfigurations]
UNION ALL
SELECT N'EntityLPOffset_SnapShot' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[EntityLPOffset_SnapShot]
UNION ALL
SELECT N'EntityOffset_SnapShot' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[EntityOffset_SnapShot]
UNION ALL
SELECT N'EntityPartnerRelationship' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[EntityPartnerRelationship]
UNION ALL
SELECT N'EntityRelationship' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[EntityRelationship]
UNION ALL
SELECT N'Enu_K1K3ValidationRule' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[Enu_K1K3ValidationRule]
UNION ALL
SELECT N'ExchangeRateOverride' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[ExchangeRateOverride]
UNION ALL
SELECT N'FDAPRate' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[FDAPRate]
UNION ALL
SELECT N'FinalFund' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[FinalFund]
UNION ALL
SELECT N'FinalFundLog' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[FinalFundLog]
UNION ALL
SELECT N'FootNoteTrialBalanceAdjustments_Snapshot' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[FootNoteTrialBalanceAdjustments_Snapshot]
UNION ALL
SELECT N'ForeignCurrencyAverageRate' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[ForeignCurrencyAverageRate]
UNION ALL
SELECT N'ForeignCurrencyRate' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[ForeignCurrencyRate]
UNION ALL
SELECT N'Form199AInput_Snapshot' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[Form199AInput_Snapshot]
UNION ALL
SELECT N'Form199ALineItem' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[Form199ALineItem]
UNION ALL
SELECT N'Form199APackage' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[Form199APackage]
UNION ALL
SELECT N'Form8865Input_Snapshot' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[Form8865Input_Snapshot]
UNION ALL
SELECT N'Form8865LineItem' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[Form8865LineItem]
UNION ALL
SELECT N'Form8865Package' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[Form8865Package]
UNION ALL
SELECT N'Form8865SchInput_Snapshot' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[Form8865SchInput_Snapshot]
UNION ALL
SELECT N'Form8865SchPackage' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[Form8865SchPackage]
UNION ALL
SELECT N'Form8886Entity' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[Form8886Entity]
UNION ALL
SELECT N'Form8886Input_Snapshot' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[Form8886Input_Snapshot]
UNION ALL
SELECT N'Form8886LineItem' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[Form8886LineItem]
UNION ALL
SELECT N'Form8886Package' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[Form8886Package]
UNION ALL
SELECT N'Form926Entity' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[Form926Entity]
UNION ALL
SELECT N'Form926Input_Snapshot' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[Form926Input_Snapshot]
UNION ALL
SELECT N'Form926LineItem' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[Form926LineItem]
UNION ALL
SELECT N'Form926Package' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[Form926Package]
UNION ALL
SELECT N'GlobalMenu' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[GlobalMenu]
UNION ALL
SELECT N'ImportErrorMessages' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[ImportErrorMessages]
UNION ALL
SELECT N'IncomeAttribute' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[IncomeAttribute]
UNION ALL
SELECT N'IncomeAttributeRounding' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[IncomeAttributeRounding]
UNION ALL
SELECT N'IncomeAttribute_PreviousYear' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[IncomeAttribute_PreviousYear]
UNION ALL
SELECT N'K1GLineItem' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[K1GLineItem]
UNION ALL
SELECT N'K1GPartnerTypes' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[K1GPartnerTypes]
UNION ALL
SELECT N'K1Input_Snapshot' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[K1Input_Snapshot]
UNION ALL
SELECT N'K1LineItem' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[K1LineItem]
UNION ALL
SELECT N'K1Package' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[K1Package]
UNION ALL
SELECT N'K1UBTI_SNAPSHOT' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[K1UBTI_SNAPSHOT]
UNION ALL
SELECT N'K3MappedAllocableLines' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[K3MappedAllocableLines]
UNION ALL
SELECT N'Location' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[Location]
UNION ALL
SELECT N'LockUnderlyingEntities' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[LockUnderlyingEntities]
UNION ALL
SELECT N'LookthroughAdjustments_Snapshot' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[LookthroughAdjustments_Snapshot]
UNION ALL
SELECT N'LookthroughReclassFNTieringBlocker_Snapshot' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[LookthroughReclassFNTieringBlocker_Snapshot]
UNION ALL
SELECT N'LookthroughReclass_Snapshot' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[LookthroughReclass_Snapshot]
UNION ALL
SELECT N'M1Adjustments_Snapshot' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[M1Adjustments_Snapshot]
UNION ALL
SELECT N'M3Rules' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[M3Rules]
UNION ALL
SELECT N'M3RulesMapping' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[M3RulesMapping]
UNION ALL
SELECT N'MAPDataRegister' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[MAPDataRegister]
UNION ALL
SELECT N'MAP_DerivedLines' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[MAP_DerivedLines]
UNION ALL
SELECT N'MAP_ImportColumn' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[MAP_ImportColumn]
UNION ALL
SELECT N'MAP_K1LineItemGroup' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[MAP_K1LineItemGroup]
UNION ALL
SELECT N'MAP_K1LineItemLineType' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[MAP_K1LineItemLineType]
UNION ALL
SELECT N'MAP_ParentChildK1GLineItem' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[MAP_ParentChildK1GLineItem]
UNION ALL
SELECT N'MAP_SourceAttributeRelation' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[MAP_SourceAttributeRelation]
UNION ALL
SELECT N'MapDefaultAllocRuleToLineItem' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[MapDefaultAllocRuleToLineItem]
UNION ALL
SELECT N'MapRulesToUnderlyings' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[MapRulesToUnderlyings]
UNION ALL
SELECT N'MapYearlyToK1' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[MapYearlyToK1]
UNION ALL
SELECT N'Map_CustomReportLine' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[Map_CustomReportLine]
UNION ALL
SELECT N'Map_Form163J' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[Map_Form163J]
UNION ALL
SELECT N'MappingLineItem' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[MappingLineItem]
UNION ALL
SELECT N'MasterChartOfAccounts' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[MasterChartOfAccounts]
UNION ALL
SELECT N'MasterImportEntityFeed' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[MasterImportEntityFeed]
UNION ALL
SELECT N'MergePartner_Snapshot' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[MergePartner_Snapshot]
UNION ALL
SELECT N'NonLookthroughEntities' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[NonLookthroughEntities]
UNION ALL
SELECT N'PE_EntityByInvestment' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[PE_EntityByInvestment]
UNION ALL
SELECT N'PFICAlertDetails' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[PFICAlertDetails]
UNION ALL
SELECT N'PFICFootNoteEntity' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[PFICFootNoteEntity]
UNION ALL
SELECT N'PFICFootnoteInput_Snapshot' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[PFICFootnoteInput_Snapshot]
UNION ALL
SELECT N'PFICFootnoteLineItem' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[PFICFootnoteLineItem]
UNION ALL
SELECT N'PFICFootnotePackage' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[PFICFootnotePackage]
UNION ALL
SELECT N'PFICK1Mapping' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[PFICK1Mapping]
UNION ALL
SELECT N'PFICUpdateAlert' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[PFICUpdateAlert]
UNION ALL
SELECT N'PFICXmlOverrideInput' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[PFICXmlOverrideInput]
UNION ALL
SELECT N'PFICXmlOverridePackage' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[PFICXmlOverridePackage]
UNION ALL
SELECT N'ParentK1GLineItem' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[ParentK1GLineItem]
UNION ALL
SELECT N'Partner_Snapshot' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[Partner_Snapshot]
UNION ALL
SELECT N'PficForeignCorpClassificationInput' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[PficForeignCorpClassificationInput]
UNION ALL
SELECT N'Phase' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[Phase]
UNION ALL
SELECT N'ProcessEntityConfigurationsImport' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[ProcessEntityConfigurationsImport]
UNION ALL
SELECT N'QuarterDates' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[QuarterDates]
UNION ALL
SELECT N'ReclassTrialBalanceAdjustments_Snapshot' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[ReclassTrialBalanceAdjustments_Snapshot]
UNION ALL
SELECT N'ReportPackage' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[ReportPackage]
UNION ALL
SELECT N'RoundingOverride_Snapshot' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[RoundingOverride_Snapshot]
UNION ALL
SELECT N'SM_AllowHighestGradualRate' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[SM_AllowHighestGradualRate]
UNION ALL
SELECT N'SM_EntityLevelStateThresholds' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[SM_EntityLevelStateThresholds]
UNION ALL
SELECT N'SM_ExcessPartnerWithholding_Snapshot' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[SM_ExcessPartnerWithholding_Snapshot]
UNION ALL
SELECT N'SM_FederaltoStatePartnerTypeMapping' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[SM_FederaltoStatePartnerTypeMapping]
UNION ALL
SELECT N'SM_K1PackageFileData' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[SM_K1PackageFileData]
UNION ALL
SELECT N'SM_LookthroughAdjustments_Snapshot' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[SM_LookthroughAdjustments_Snapshot]
UNION ALL
SELECT N'SM_LookthroughReclass_Snapshot' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[SM_LookthroughReclass_Snapshot]
UNION ALL
SELECT N'SM_PYFiling' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[SM_PYFiling]
UNION ALL
SELECT N'SM_PYFiling_Snapshot' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[SM_PYFiling_Snapshot]
UNION ALL
SELECT N'SM_PartnerComposite' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[SM_PartnerComposite]
UNION ALL
SELECT N'SM_PartnerComposite_Snapshot' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[SM_PartnerComposite_Snapshot]
UNION ALL
SELECT N'SM_PartnerWithholding_Snapshot' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[SM_PartnerWithholding_Snapshot]
UNION ALL
SELECT N'SM_PaymentAllocationRules' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[SM_PaymentAllocationRules]
UNION ALL
SELECT N'SM_Payment_Snapshot' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[SM_Payment_Snapshot]
UNION ALL
SELECT N'SM_RoundingOverride' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[SM_RoundingOverride]
UNION ALL
SELECT N'SM_SpecialAllocationCode' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[SM_SpecialAllocationCode]
UNION ALL
SELECT N'SM_SpecialAllocationInput_Snapshot' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[SM_SpecialAllocationInput_Snapshot]
UNION ALL
SELECT N'SM_SpecialAllocationPaymentLineItem' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[SM_SpecialAllocationPaymentLineItem]
UNION ALL
SELECT N'SM_StateCompositeConfiguration' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[SM_StateCompositeConfiguration]
UNION ALL
SELECT N'SM_StateFilingHighPenaltyLogic' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[SM_StateFilingHighPenaltyLogic]
UNION ALL
SELECT N'SM_StateFilingResidencyLogic' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[SM_StateFilingResidencyLogic]
UNION ALL
SELECT N'SM_StateLineAllocationRule_Snapshot' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[SM_StateLineAllocationRule_Snapshot]
UNION ALL
SELECT N'SM_StateLines' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[SM_StateLines]
UNION ALL
SELECT N'SM_StateTaxRates' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[SM_StateTaxRates]
UNION ALL
SELECT N'SM_StateTaxRates_SubLineItems' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[SM_StateTaxRates_SubLineItems]
UNION ALL
SELECT N'SM_StateThresholds' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[SM_StateThresholds]
UNION ALL
SELECT N'SM_StateWorkspaceInput_Snapshot' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[SM_StateWorkspaceInput_Snapshot]
UNION ALL
SELECT N'SM_WHWaiverExemptRules' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[SM_WHWaiverExemptRules]
UNION ALL
SELECT N'SidePocketDefinition' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[SidePocketDefinition]
UNION ALL
SELECT N'StateWithholdingCompositeOverrideData_Snapshot' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[StateWithholdingCompositeOverrideData_Snapshot]
UNION ALL
SELECT N'State_StateCode' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[State_StateCode]
UNION ALL
SELECT N'TU_Entity' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[TU_Entity]
UNION ALL
SELECT N'TagPercentage_Snapshot' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[TagPercentage_Snapshot]
UNION ALL
SELECT N'TaxHoldbackPercentage' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[TaxHoldbackPercentage]
UNION ALL
SELECT N'TaxPeriod' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[TaxPeriod]
UNION ALL
SELECT N'Tranche' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[Tranche]
UNION ALL
SELECT N'TrancheSidePocket' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[TrancheSidePocket]
UNION ALL
SELECT N'TransactionLog' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[TransactionLog]
UNION ALL
SELECT N'TransfersInput_Snapshot' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[TransfersInput_Snapshot]
UNION ALL
SELECT N'Transfers_Snapshot' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[Transfers_Snapshot]
UNION ALL
SELECT N'TrialBalanceAdjustments_SnapShot' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[TrialBalanceAdjustments_SnapShot]
UNION ALL
SELECT N'UBTIDFPercent_Snapshot' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[UBTIDFPercent_Snapshot]
UNION ALL
SELECT N'UBTILOC_Snapshot' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[UBTILOC_Snapshot]
UNION ALL
SELECT N'Workflow' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[Workflow]
UNION ALL
SELECT N'WorkflowChain' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[WorkflowChain]
UNION ALL
SELECT N'WorkflowStatus' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[WorkflowStatus]
UNION ALL
SELECT N'YearlyByInvestment' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[YearlyByInvestment]
UNION ALL
SELECT N'Yearly_Snapshot' AS table_nm, COUNT_BIG(*) AS row_count FROM dbo.[Yearly_Snapshot]
ORDER BY table_nm;
GO
