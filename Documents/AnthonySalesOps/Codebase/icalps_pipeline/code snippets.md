
import dlt

import requests

import json

from pathlib import Path

import os

  
  

def _get_ads(url_for_search, params):

    headers = {"accept": "application/json"}

    response = requests.get(url_for_search, headers=headers, params=params)

    response.raise_for_status()  # check for http errors

    return json.loads(response.content.decode("utf8"))

  
  

@dlt.resource(write_disposition="append")

def jobsearch_resource(params):

    """

    params should include at least:

      - "q": your query

      - "limit": page size (e.g. 100)

    """

    url = "https://jobsearch.api.jobtechdev.se"

    url_for_search = f"{url}/search"

    limit = params.get("limit", 100)

    offset = 0

  

    while True:

        # build this page’s params

        page_params = dict(params, offset=offset)

        data = _get_ads(url_for_search, page_params)

  

        hits = data.get("hits", [])

        if not hits:

            # no more results

            break

  

        # yield each ad on this page

        for ad in hits:

            yield ad

  

        # if fewer than a full page was returned, we’re done

        if len(hits) < limit or offset > 1900:

            break

  

        offset += limit

  
  

def run_pipeline(query, table_name, occupation_fields):

    pipeline = dlt.pipeline(

        pipeline_name="jobads_demo",

        destination=dlt.destinations.duckdb("ads_data_warehouse.duckdb"),

        dataset_name="staging",

    )

  

    for occupation_field in occupation_fields:

        params = {"q": query, "limit": 100, "occupation-field": occupation_field}

        load_info = pipeline.run(

            jobsearch_resource(params=params), table_name=table_name

        )

        print(f"Occupation field: {occupation_field}")

        print(load_info)

  
  

if __name__ == "__main__":

    working_directory = Path(__file__).parent

    os.chdir(working_directory)

  

    query = ""

    table_name = "job_ads"

  

    # Teknisk inriktning, "Hälso sjukvård", "Pedagogik"

    occupation_fields = ("6Hq3_tKo_V57", "NYW6_mP6_vwf", "MVqp_eS8_kDZ")

  

    run_pipeline(query, table_name, occupation_fields)




## SAfe field mapping

# Load required libraries
library(readxl)
library(dplyr)
library(stringr)
library(readr)
library(tibble)

# Step 8: Generate field mapping documentation from Excel workbook
cat("Step 8: Field mapping documentation from workbook...\n")

# Read headers from both sheets
workbook_path <- "Book1.xlsx"  # Update path as needed

# Read headers from Legacy sheet (assuming headers are in row 1)
legacy_headers <- readxl::read_excel(workbook_path, sheet = "Legacy", n_max = 1) %>%
  names()

# Read headers from Hubspot sheet (assuming headers are in row 1)
hubspot_headers <- readxl::read_excel(workbook_path, sheet = "Hubspot", n_max = 1) %>%
  names()

cat("Legacy headers found:", length(legacy_headers), "\n")
cat("HubSpot headers found:", length(hubspot_headers), "\n")

# Create mapping rules based on semantic and business logic
create_mapping_rules <- function(legacy_field) {
  legacy_lower <- tolower(legacy_field)
  
  # Define mapping logic based on field semantics using correct HubSpot conventions
  if (str_detect(legacy_lower, "record.*id|id.*record")) {
    list(
      hubspot_field = "Record ID",
      mapping_type = "Direct",
      transformation_logic = "1:1 mapping (optional - only if source field exists)"
    )
  } else if (str_detect(legacy_lower, "subject")) {
    list(
      hubspot_field = "Communication body",
      mapping_type = "Semantic Combination",
      transformation_logic = "Subject + Notes concatenation"
    )
  } else if (str_detect(legacy_lower, "notes|comment|body")) {
    list(
      hubspot_field = "Communication body",
      mapping_type = "Semantic Combination", 
      transformation_logic = "Subject + Notes concatenation"
    )
  } else if (str_detect(legacy_lower, "source|channel")) {
    list(
      hubspot_field = "Channel Type",
      mapping_type = "Semantic Classification",
      transformation_logic = "Email/Phone/Meeting classification"
    )
  } else if (str_detect(legacy_lower, "communication.*type|type.*communication")) {
    list(
      hubspot_field = "Channel Type",
      mapping_type = "Semantic Classification",
      transformation_logic = "Inbound/Outbound classification"
    )
  } else if (str_detect(legacy_lower, "status")) {
    list(
      hubspot_field = "Record source",
      mapping_type = "Derived",
      transformation_logic = "Status derivation"
    )
  } else if (str_detect(legacy_lower, "date") & str_detect(legacy_lower, "communication|activity")) {
    list(
      hubspot_field = "Activity date",
      mapping_type = "Direct",
      transformation_logic = "Date format preservation"
    )
  } else if (str_detect(legacy_lower, "create.*date|date.*create|modified")) {
    list(
      hubspot_field = "Object last modified date/time",
      mapping_type = "Timestamp",
      transformation_logic = "Current timestamp"
    )
  } else if (str_detect(legacy_lower, "company.*name")) {
    list(
      hubspot_field = "Associated Company",
      mapping_type = "Lookup Join",
      transformation_logic = "Company name lookup via ID"
    )
  } else if (str_detect(legacy_lower, "first.*name|name.*first")) {
    list(
      hubspot_field = "Associated Contact",
      mapping_type = "Lookup Join",
      transformation_logic = "Contact name lookup via ID"
    )
  } else if (str_detect(legacy_lower, "last.*name|name.*last")) {
    list(
      hubspot_field = "Associated Contact",
      mapping_type = "Lookup Join",
      transformation_logic = "Contact name lookup via ID"
    )
  } else if (str_detect(legacy_lower, "company.*id")) {
    list(
      hubspot_field = "Associated Company IDs",
      mapping_type = "Foreign Key Lookup",
      transformation_logic = "Legacy to new ID mapping"
    )
  } else if (str_detect(legacy_lower, "contact.*id")) {
    list(
      hubspot_field = "Associated Contact IDs",
      mapping_type = "Foreign Key Lookup",
      transformation_logic = "Legacy to new ID mapping"
    )
  } else if (str_detect(legacy_lower, "deal.*id|opportunity.*id")) {
    list(
      hubspot_field = "Associated Deal IDs",
      mapping_type = "Foreign Key Lookup",
      transformation_logic = "Legacy to new ID mapping"
    )
  } else if (str_detect(legacy_lower, "conversation.*id|thread.*id")) {
    list(
      hubspot_field = "Associated Conversation IDs",
      mapping_type = "Foreign Key Lookup",
      transformation_logic = "Legacy to new ID mapping"
    )
  } else if (str_detect(legacy_lower, "deal|opportunity")) {
    list(
      hubspot_field = "Associated Deal",
      mapping_type = "Lookup Join",
      transformation_logic = "Deal lookup via ID"
    )
  } else if (str_detect(legacy_lower, "conversation|thread")) {
    list(
      hubspot_field = "Associated Conversation",
      mapping_type = "Lookup Join",
      transformation_logic = "Conversation lookup via ID"
    )
  } else if (str_detect(legacy_lower, "priority")) {
    list(
      hubspot_field = "Record source",
      mapping_type = "Derived",
      transformation_logic = "Business rules application"
    )
  } else if (str_detect(legacy_lower, "from|assigned.*from")) {
    list(
      hubspot_field = "Activity assigned to",
      mapping_type = "Conditional Logic",
      transformation_logic = "From/To conditional on direction"
    )
  } else if (str_detect(legacy_lower, "to|assigned.*to")) {
    list(
      hubspot_field = "Activity assigned to",
      mapping_type = "Conditional Logic",
      transformation_logic = "From/To conditional on direction"
    )
  } else {
    # Default mapping for unrecognized fields
    list(
      hubspot_field = "unmapped_field",
      mapping_type = "Manual Review Required",
      transformation_logic = "Field requires manual mapping"
    )
  }
}

# Apply mapping rules to all legacy headers using dplyr
field_mapping_doc <- tibble(Legacy_Field = legacy_headers) %>%
  rowwise() %>%
  mutate(
    mapping_result = list(create_mapping_rules(Legacy_Field))
  ) %>%
  mutate(
    New_CRM_Field = mapping_result$hubspot_field,
    Mapping_Type = mapping_result$mapping_type,
    Transformation_Logic = mapping_result$transformation_logic
  ) %>%
  select(-mapping_result) %>%
  ungroup()

# Add validation to check if mapped fields exist in HubSpot headers
field_mapping_doc <- field_mapping_doc %>%
  mutate(
    Field_Exists_in_Target = case_when(
      str_detect(New_CRM_Field, "\\(") ~ "Derived Field",
      New_CRM_Field %in% hubspot_headers ~ "Yes",
      TRUE ~ "No"
    )
  )

# Display results
cat("\nFIELD MAPPING DOCUMENTATION:\n")
print(field_mapping_doc)

# Summary statistics
cat("\nMAPPING SUMMARY:\n")
field_mapping_doc %>%
  count(Mapping_Type, name = "Count") %>%
  arrange(desc(Count)) %>%
  print()

# Fields requiring manual review
manual_review_fields <- field_mapping_doc %>%
  filter(Mapping_Type == "Manual Review Required") %>%
  pull(Legacy_Field)

if (length(manual_review_fields) > 0) {
  cat("\nFIELDS REQUIRING MANUAL REVIEW:\n")
  cat(paste(manual_review_fields, collapse = ", "))
  cat("\n")
}

# Check for unmapped HubSpot fields
unmapped_hubspot <- setdiff(hubspot_headers, field_mapping_doc$New_CRM_Field)
if (length(unmapped_hubspot) > 0) {
  cat("\nHUBSPOT FIELDS NOT MAPPED FROM LEGACY:\n")
  cat(paste(unmapped_hubspot, collapse = ", "))
  cat("\n")
}

# Export mapping documentation
write.csv(field_mapping_doc, "field_mapping_documentation.csv", row.names = FALSE)
cat("\nMapping documentation exported to: field_mapping_documentation.csv\n")

# ===================================================================
# STEP 9: DATA TRANSFORMATION LAYER
# ===================================================================
cat("\nStep 9: Applying transformations to actual data...\n")

# Modern transformation  and normalisation function using dplyr and temporary tables
transform_legacy_data <- function(legacy_data_path = NULL, legacy_data = NULL) {
  
  # Read legacy data if path provided, otherwise use provided data
  if (!is.null(legacy_data_path)) {
    legacy_tbl <- read.csv(legacy_data_path, stringsAsFactors = FALSE) %>% as_tibble()
  } else if (!is.null(legacy_data)) {
    legacy_tbl <- as_tibble(legacy_data)
  } else {
    stop("Either legacy_data_path or legacy_data must be provided")
  }
  
  cat("Processing", nrow(legacy_tbl), "legacy records...\n")
  
  # Create mapping reference table for transformations
  transformation_map <- field_mapping_doc %>%
    filter(New_CRM_Field != "unmapped_field") %>%
    mutate(
      legacy_col_clean = str_replace_all(Legacy_Field, "\\s+", "_"),
      hubspot_col_clean = str_replace_all(New_CRM_Field, "\\s+", "_")
    )
  
  # Step 1: Create base transformation table with standardized column names
  base_tbl <- legacy_tbl %>%
    rename_with(~ str_replace_all(.x, "\\s+", "_")) %>%
    mutate(row_id = row_number())
  
  # Step 2: Apply direct mappings using case_when and temporary tables
  direct_mappings <- transformation_map %>%
    filter(Mapping_Type == "Direct") %>%
    pull(Legacy_Field, name = New_CRM_Field)
  
  # Create direct mapping table (only if Record ID exists)
  has_record_id <- "Record_ID" %in% names(base_tbl)
  
  if(has_record_id) {
    direct_temp <- base_tbl %>%
      transmute(
        row_id,
        Record_ID = as.character(Record_ID)
      )
  } else {
    direct_temp <- base_tbl %>%
      transmute(row_id)
  }
  
  # Step 3: Communication body semantic combination
  communication_temp <- base_tbl %>%
    transmute(
      row_id,
      Communication_body = case_when(
        "Communication_Subject" %in% names(.) & "Communication_Notes" %in% names(.) &
        !is.na(Communication_Subject) & !is.na(Communication_Notes) & 
        nchar(trimws(Communication_Notes)) > 0 ~ 
          paste(trimws(Communication_Subject), trimws(Communication_Notes), sep = " | "),
        "Communication_Subject" %in% names(.) & !is.na(Communication_Subject) ~ 
          trimws(Communication_Subject),
        "Communication_Notes" %in% names(.) & !is.na(Communication_Notes) ~ 
          trimws(Communication_Notes),
        TRUE ~ ""
      ) %>% str_remove("^\\s*\\|\\s*|\\s*\\|\\s*$")
    )
  
  # Step 4: Channel type classification
  channel_temp <- base_tbl %>%
    transmute(
      row_id,
      Channel_Type = case_when(
        "Communication_Source" %in% names(.) & 
        str_to_lower(Communication_Source) %in% c("email", "e-mail", "mail") ~ "Email",
        "Communication_Source" %in% names(.) & 
        str_to_lower(Communication_Source) %in% c("phone", "call", "telephone", "tel") ~ "Phone",
        "Communication_Source" %in% names(.) & 
        str_to_lower(Communication_Source) %in% c("meeting", "visit", "appointment", "face-to-face") ~ "Meeting",
        "Communication_Source" %in% names(.) & 
        str_to_lower(Communication_Source) %in% c("task", "todo", "action", "reminder") ~ "Task",
        "Communication_Source" %in% names(.) & 
        str_to_lower(Communication_Source) %in% c("note", "comment", "memo") ~ "Note",
        "Communication_Source" %in% names(.) & 
        str_to_lower(Communication_Source) %in% c("sms", "text", "message") ~ "SMS",
        "Communication_Source" %in% names(.) & 
        str_to_lower(Communication_Source) %in% c("social", "linkedin", "twitter") ~ "Social Media",
        "Communication_Source" %in% names(.) ~ "Other",
        TRUE ~ "Unknown"
      )
    )
  
  # Step 5: Status and priority derivation
  status_priority_temp <- base_tbl %>%
    transmute(
      row_id,
      Record_source = case_when(
        "Communication_Status" %in% names(.) & 
        str_to_lower(Communication_Status) %in% c("completed", "done", "finished", "closed") ~ "Completed",
        "Communication_Status" %in% names(.) & 
        str_to_lower(Communication_Status) %in% c("pending", "in progress", "ongoing", "active") ~ "In Progress",
        "Communication_Status" %in% names(.) & 
        str_to_lower(Communication_Status) %in% c("cancelled", "canceled", "stopped", "aborted") ~ "Cancelled",
        "Communication_Status" %in% names(.) & 
        str_to_lower(Communication_Status) %in% c("planned", "scheduled", "future", "upcoming") ~ "Scheduled",
        "Communication_Status" %in% names(.) & 
        str_to_lower(Communication_Status) %in% c("new", "open", "unread") ~ "Not Started",
        "Communication_Status" %in% names(.) ~ "Not Started",
        TRUE ~ "Unknown"
      ),
      Priority_normalized = case_when(
        "Priority" %in% names(.) & 
        str_to_lower(Priority) %in% c("high", "urgent", "critical", "important") ~ "High",
        "Priority" %in% names(.) & 
        str_to_lower(Priority) %in% c("normal", "medium", "standard", "regular") ~ "Medium", 
        "Priority" %in% names(.) & 
        str_to_lower(Priority) %in% c("low", "minor", "trivial") ~ "Low",
        "Priority" %in% names(.) ~ "Medium",
        TRUE ~ "Medium"
      )
    )
  
  # Step 6: Date transformations
  dates_temp <- base_tbl %>%
    transmute(
      row_id,
      Activity_date = case_when(
        "Communication_Date" %in% names(.) & !is.na(Communication_Date) ~ tryCatch({
          # Handle Excel decimal dates
          as.Date(as.numeric(Communication_Date), origin = "1899-12-30")
        }, error = function(e) as.Date(NA)),
        TRUE ~ as.Date(NA)
      ),
      Object_last_modified_date_time = case_when(
        "Create_Date" %in% names(.) & !is.na(Create_Date) ~ tryCatch({
          as.POSIXct(as.Date(as.numeric(Create_Date), origin = "1899-12-30"))
        }, error = function(e) Sys.time()),
        TRUE ~ Sys.time()
      )
    )
  
  # Step 7: Contact and company associations
  associations_temp <- base_tbl %>%
    transmute(
      row_id,
      Associated_Contact = case_when(
        "Contact_First_Name" %in% names(.) & "Contact_Last_Name" %in% names(.) &
        !is.na(Contact_First_Name) & !is.na(Contact_Last_Name) ~ 
          paste(trimws(Contact_First_Name), trimws(Contact_Last_Name)),
        "Contact_First_Name" %in% names(.) & !is.na(Contact_First_Name) ~ 
          trimws(Contact_First_Name),
        "Contact_Last_Name" %in% names(.) & !is.na(Contact_Last_Name) ~ 
          trimws(Contact_Last_Name),
        TRUE ~ ""
      ),
      Associated_Company = case_when(
        "Company_Name" %in% names(.) & !is.na(Company_Name) ~ trimws(Company_Name),
        TRUE ~ ""
      ),
      Associated_Contact_IDs = case_when(
        "Contact_ID" %in% names(.) & !is.na(Contact_ID) ~ as.character(Contact_ID),
        TRUE ~ ""
      ),
      Associated_Company_IDs = case_when(
        "Company_ID" %in% names(.) & !is.na(Company_ID) ~ as.character(Company_ID),
        TRUE ~ ""
      )
    )
  
  # Step 8: Activity assignment logic
  activity_temp <- base_tbl %>%
    transmute(
      row_id,
      Activity_assigned_to = case_when(
        # If it's an outbound communication, assign to current user
        "Communication_Type" %in% names(.) & 
        str_to_lower(Communication_Type) %in% c("outbound", "call", "task") ~ "Current User",
        # If it's an inbound communication, assign based on contact name
        "Communication_Type" %in% names(.) & 
        str_to_lower(Communication_Type) %in% c("inbound", "email") & 
        ("Contact_First_Name" %in% names(.) | "Contact_Last_Name" %in% names(.)) ~ 
          case_when(
            "Contact_First_Name" %in% names(.) & "Contact_Last_Name" %in% names(.) ~
              paste(ifelse(is.na(Contact_First_Name), "", Contact_First_Name), 
                    ifelse(is.na(Contact_Last_Name), "", Contact_Last_Name)) %>% str_trim(),
            "Contact_First_Name" %in% names(.) ~
              ifelse(is.na(Contact_First_Name), "", Contact_First_Name) %>% str_trim(),
            "Contact_Last_Name" %in% names(.) ~
              ifelse(is.na(Contact_Last_Name), "", Contact_Last_Name) %>% str_trim(),
            TRUE ~ ""
          ),
        # Default assignment
        TRUE ~ "Unassigned"
      )
    )
  
  # Step 9: Assemble final HubSpot format using temporary table joins
  hubspot_base <- base_tbl %>%
    select(row_id) %>%
    left_join(direct_temp, by = "row_id") %>%
    left_join(communication_temp, by = "row_id") %>%
    left_join(channel_temp, by = "row_id") %>%
    left_join(status_priority_temp, by = "row_id") %>%
    left_join(dates_temp, by = "row_id") %>%
    left_join(associations_temp, by = "row_id") %>%
    left_join(activity_temp, by = "row_id")
  
  # Create final structure
  if(has_record_id) {
    hubspot_final <- hubspot_base %>%
      transmute(
        `Record ID` = Record_ID,
        `Channel Type` = Channel_Type,
        `Communication body` = Communication_body,
        `Associated Contact` = Associated_Contact,
        `Activity assigned to` = Activity_assigned_to,
        `Activity date` = Activity_date,
        `Associated Company` = Associated_Company,
        `Associated Deal` = "",
        `Associated Conversation` = "",
        `Object last modified date/time` = Object_last_modified_date_time,
        `Record source` = Record_source,
        `Associated Contact IDs` = Associated_Contact_IDs,
        `Associated Company IDs` = Associated_Company_IDs,
        `Associated Deal IDs` = "",
        `Associated Conversation IDs` = ""
      )
  } else {
    hubspot_final <- hubspot_base %>%
      transmute(
        `Channel Type` = Channel_Type,
        `Communication body` = Communication_body,
        `Associated Contact` = Associated_Contact,
        `Activity assigned to` = Activity_assigned_to,
        `Activity date` = Activity_date,
        `Associated Company` = Associated_Company,
        `Associated Deal` = "",
        `Associated Conversation` = "",
        `Object last modified date/time` = Object_last_modified_date_time,
        `Record source` = Record_source,
        `Associated Contact IDs` = Associated_Contact_IDs,
        `Associated Company IDs` = Associated_Company_IDs,
        `Associated Deal IDs` = "",
        `Associated Conversation IDs` = ""
      )
  }
  
  # Apply final cleaning
  hubspot_final <- hubspot_final %>%
    mutate(across(where(is.character), ~ ifelse(is.na(.x) | .x == "", "", str_trim(.x))))
  
  # Step 10: Data quality validation and summary
  validation_metrics <- c("Total Records", "Complete Records", "Missing Communication Body", 
                          "Missing Contact Association", "Missing Company Association", "Invalid Dates")
  validation_counts <- c(
    nrow(hubspot_final),
    nrow(hubspot_final %>% filter(`Communication body` != "")),
    nrow(hubspot_final %>% filter(`Communication body` == "" | is.na(`Communication body`))),
    nrow(hubspot_final %>% filter(`Associated Contact` == "" | is.na(`Associated Contact`))),
    nrow(hubspot_final %>% filter(`Associated Company` == "" | is.na(`Associated Company`))),
    nrow(hubspot_final %>% filter(is.na(`Activity date`)))
  )
  
  # Add Record ID validation only if Record ID column exists
  if("Record ID" %in% names(hubspot_final)) {
    validation_metrics <- c(validation_metrics, "Missing Record ID")
    validation_counts <- c(validation_counts, 
                          nrow(hubspot_final %>% filter(is.na(`Record ID`) | `Record ID` == "")))
  }
  
  validation_summary <- tibble(
    metric = validation_metrics,
    count = validation_counts
  )
  
  cat("\nDATA QUALITY VALIDATION:\n")
  print(validation_summary)
  
  return(hubspot_final)
}

# Example usage with sample data (using tibble for modern approach, no Record ID)
sample_data <- tibble(
  `Communication Subject` = c("Suivi Bretagnol", "RE: REOI PEPPER", "Re: Démarrage de WALLIS ?"),
  `Communication Notes` = c("Le projet ASIC est abandonné", "", ""),
  `Communication Source` = c("Phone", "Email", "Email"),
  `Communication Type` = c("Task", "Email", "Email"),
  `Communication Status` = c("Pending", "Completed", "Completed"),
  `Communication Date` = c("44927", "44943", "44942"),  # Excel date numbers
  `Create Date` = c("44944", "44943", "44940"),
  `Company Name` = c("VALEO Issoire", "IMEC ASIC Services", "Wise Integration"),
  `Contact First Name` = c("Frédéric", "Paul", "Thierry"),
  `Contact Last Name` = c("BRETAGNOL", "ZUBER", "BOUCHET"),
  `Company ID` = c("18610", "4282", "15153"),
  `Contact ID` = c("22025", "4519", "18072"),
  `Priority` = c("Normal", "Normal", "Normal")
)

# Apply modern transformation to sample data
cat("\nApplying modern transformations to sample data...\n")
transformed_data <- transform_legacy_data(legacy_data = sample_data)

# Display transformation results with modern formatting
cat("\nTRANSFORMED DATA PREVIEW:\n")
transformed_data %>%
  slice_head(n = 3) %>%
  glimpse()

# Create transformation statistics table (without Record ID dependency)
complete_records_count <- if("Record ID" %in% names(transformed_data)) {
  nrow(transformed_data %>% filter(!is.na(`Record ID`), `Communication body` != ""))
} else {
  nrow(transformed_data %>% filter(`Communication body` != ""))
}

data_quality_score <- if("Record ID" %in% names(transformed_data)) {
  round(nrow(transformed_data %>% filter(!is.na(`Record ID`), `Communication body` != "")) / 
        nrow(transformed_data) * 100, 1)
} else {
  round(nrow(transformed_data %>% filter(`Communication body` != "")) / 
        nrow(transformed_data) * 100, 1)
}

transformation_stats <- tibble(
  Metric = c("Original Columns", "Transformed Columns", "Records Processed", 
             "Complete Records", "Data Quality Score"),
  Value = c(
    ncol(sample_data),
    ncol(transformed_data),
    nrow(transformed_data),
    complete_records_count,
    data_quality_score
  ),
  Unit = c("count", "count", "count", "count", "percent")
)

cat("\nTRANSFORMATION STATISTICS:\n")
print(transformation_stats)

# Export enhanced results
write_csv(transformed_data, "hubspot_transformed_data.csv")
write_csv(transformation_stats, "transformation_statistics.csv")

cat("\nFiles exported:\n")
cat("- hubspot_transformed_data.csv (Transformed data)\n")
cat("- transformation_statistics.csv (Process metrics)\n")
cat("- field_mapping_documentation.csv (Mapping rules)\n")

# Display column structure info
if("Record ID" %in% names(transformed_data)) {
  cat("- Record ID column: INCLUDED (found in source data)\n")
} else {
  cat("- Record ID column: EXCLUDED (not found in source data)\n")
}

# Final summary using modern dplyr approach (without Record ID dependency)
final_summary <- transformed_data %>%
  summarise(
    total_records = n(),
    records_with_communication = sum(!is.na(`Communication body`) & `Communication body` != ""),
    records_with_contact = sum(!is.na(`Associated Contact`) & `Associated Contact` != ""),
    records_with_company = sum(!is.na(`Associated Company`) & `Associated Company` != ""),
    unique_channels = n_distinct(`Channel Type`, na.rm = TRUE),
    date_range_days = case_when(
      all(is.na(`Activity date`)) ~ 0,
      TRUE ~ as.numeric(max(`Activity date`, na.rm = TRUE) - min(`Activity date`, na.rm = TRUE))
    ),
    .groups = "drop"
  )

cat("\nFINAL SUMMARY:\n")
print(final_summary)



# Create custom columns for valuation

let
    // Start from the merged dataset (Legacy CRM + Current CRM)
    Source = Table.NestedJoin(Table1, {"Oppo_OpportunityId"}, Table3, {"icalps_deal_id"}, "Table3", JoinKind.LeftOuter),
    #"Expanded Table3" = Table.ExpandTableColumn(Source, "Table3", {"Record ID", "Deal Name", "Deal Stage", "Pipeline", "icalps_company_id", "icalps_contact_id", "icalps_deal_id"}, {"Record ID", "Deal Name", "Deal Stage", "Pipeline", "icalps_company_id", "icalps_contact_id", "icalps_deal_id"}),
    
    // Rename legacy forecast to Amount for consistency
    #"Renamed Legacy Amount" = Table.RenameColumns(#"Expanded Table3",{{"Oppo_Forecast", "Legacy_Amount"}}),
    
    // Stage mapping function - Legacy to IC'ALPS structure
    MapStage = (stage as nullable text) as text =>
        if stage = null then ""
        else if stage = "Identification" then "01 - Identification"
        else if stage = "Qualified" then "02 - Qualifiée"
        else if stage = "Evaluation technique" then "03 - Evaluation technique"
        else if stage = "Construction offre" then "04 - Construction propositions"
        else if stage = "Negotiating" then "05 - Négociations"
        else stage, // Keep original value if no mapping found
    
    // Status mapping function (keeping original values as they are already correct)
    MapStatus = (status as nullable text) as text =>
        if status = null then ""
        else status, // Status values are already correct: In Progress, Abandonne, Lost, Won, NoGo
    
    // Smart semantic mapping function for CRM Deal Stages
    MapDealStage = (pipeline as nullable text, stage as nullable text, status as nullable text) as text =>
        let
            // Normalize inputs
            clean_pipeline = if pipeline = null then "" else Text.Lower(Text.Trim(pipeline)),
            clean_stage = if stage = null then "" else Text.Trim(stage),
            clean_status = if status = null then "" else Text.Lower(Text.Trim(status)),
            
            // Determine if it's Hardware or Software pipeline
            is_hardware = Text.Contains(clean_pipeline, "hardware") or Text.Contains(clean_pipeline, "icalps_hardware"),
            is_software = Text.Contains(clean_pipeline, "software") or Text.Contains(clean_pipeline, "service") or Text.Contains(clean_pipeline, "icalps_service"),
            
            result = 
                // Handle universal outcomes first (regardless of pipeline)
                if clean_status = "perdue" or clean_status = "lost" then "Closed Lost"
                else if clean_status = "abandonnée" or clean_status = "abandonne" or clean_status = "nogo" then "Closed Lost"
                
                // Hardware Pipeline Logic
                else if is_hardware then
                    if clean_status = "gagnée" or clean_status = "won" then
                        // Hardware wins - map based on stage maturity
                        if Text.Contains(clean_stage, "01 - identification") then "Identified"
                        else if Text.Contains(clean_stage, "02 - qualifiée") then "Qualified"
                        else if Text.Contains(clean_stage, "03 - evaluation") then "Design Win"
                        else if Text.Contains(clean_stage, "04 - construction") or Text.Contains(clean_stage, "05 - négociations") then "Closed Won"
                        else "Closed Won"
                    else if clean_status = "en cours" or clean_status = "in progress" then
                        // Hardware in progress - map based on stage
                        if Text.Contains(clean_stage, "01 - identification") then "Identified"
                        else if Text.Contains(clean_stage, "02 - qualifiée") then "Qualified"
                        else if Text.Contains(clean_stage, "03 - evaluation") then "Design In"
                        else if Text.Contains(clean_stage, "04 - construction") then "Design In"
                        else if Text.Contains(clean_stage, "05 - négociations") then "Design Win"
                        else "Design In"
                    else "On-Hold"
                
                // Software Pipeline Logic  
                else if is_software then
                    if clean_status = "gagnée" or clean_status = "won" then "Closed Won"
                    else if clean_status = "en cours" or clean_status = "in progress" then
                        // Software in progress - simpler mapping
                        if Text.Contains(clean_stage, "01 - identification") then "Identification"
                        else if Text.Contains(clean_stage, "02 - qualifiée") or 
                               Text.Contains(clean_stage, "03 - evaluation") or
                               Text.Contains(clean_stage, "04 - construction") or
                               Text.Contains(clean_stage, "05 - négociations") then "Design Win"
                        else "Identified"
                    else "On-Hold"
                
                // Default fallback
                else if clean_status = "gagnée" or clean_status = "won" then "Closed Won"
                else if clean_status = "en cours" or clean_status = "in progress" then "Identified"
                else "On-Hold"
        in
            result,
    
    // Add mapped legacy stage column
    #"Added Mapped Legacy Stage" = Table.AddColumn(#"Renamed Legacy Amount", "Legacy_Mapped_Stage", each 
        MapStage([Oppo_Stage]), type text),
    
    // Add mapped legacy status column  
    #"Added Mapped Legacy Status" = Table.AddColumn(#"Added Mapped Legacy Stage", "Legacy_Mapped_Status", each 
        MapStatus([Oppo_Status]), type text),
    
    // Determine pipeline type based on opportunity type or product
     #"Added Pipeline Type" = Table.AddColumn(#"Added Mapped Legacy Status", "Pipeline_Type", each 
        let
            // Get deal name and normalize it
            deal_name = if [Deal Name] = null then "" else Text.Lower(Text.Trim([Deal Name])),
            
            // Check for service/etude variations
            contains_etude = Text.Contains(deal_name, "etude") or 
                           Text.Contains(deal_name, "étude") or
                           Text.Contains(deal_name, "etudes") or
                           Text.Contains(deal_name, "études"),
            
            contains_pre_etude = Text.Contains(deal_name, "pre-etude") or
                               Text.Contains(deal_name, "pré-etude") or
                               Text.Contains(deal_name, "pre-étude") or
                               Text.Contains(deal_name, "pré-étude") or
                               Text.Contains(deal_name, "preetude") or
                               Text.Contains(deal_name, "préetude") or
                               Text.Contains(deal_name, "pre etude") or
                               Text.Contains(deal_name, "pré etude"),
            
            contains_faisabilite = Text.Contains(deal_name, "faisabilité") or
                                 Text.Contains(deal_name, "faisabilite"),
            
            // Additional service keywords
            contains_service = Text.Contains(deal_name, "service") or
                             Text.Contains(deal_name, "consulting") or
                             Text.Contains(deal_name, "conseil")
        in
            // Map to pipeline type
            if contains_etude or contains_pre_etude or contains_faisabilite or contains_service then 
                "icalps_service"
            else 
                "icalps_hardware", type text),
    
    // Apply smart semantic mapping to get predicted CRM Deal Stage
    #"Added Predicted CRM Stage" = Table.AddColumn(#"Added Pipeline Type", "Predicted_CRM_Deal_Stage", each 
        MapDealStage([Pipeline_Type], [Legacy_Mapped_Stage], [Legacy_Mapped_Status]), type text),
    
    // Add OppoStage concatenation column (for future uploads)
    #"Added OppoStage" = Table.AddColumn(#"Added Predicted CRM Stage", "OppoStage", each 
        if [Legacy_Mapped_Stage] = "" or [Legacy_Mapped_Status] = "" then 
            ""
        else 
            [Legacy_Mapped_Stage] & " - " & [Legacy_Mapped_Status], type text),
    
    // Add comparison column to see if prediction matches actual
    #"Added Stage Comparison" = Table.AddColumn(#"Added OppoStage", "Stage_Match", each 
        if [Deal Stage] = null then "No Current CRM Data"
        else if [Predicted_CRM_Deal_Stage] = [Deal Stage] then "Match"
        else "Mismatch: " & [Predicted_CRM_Deal_Stage] & " vs " & [Deal Stage], type text),
    
    // Financial calculations
    #"Added Net Amount" = Table.AddColumn(#"Added Stage Comparison", "Net_Legacy_Amount", each 
        [Legacy_Amount] - [oppo_cout], type number),
    
    #"Added Weighted Amount" = Table.AddColumn(#"Added Net Amount", "Weighted_Legacy_Amount", each 
        [Legacy_Amount] * [Oppo_Certainty], type number),
    
    #"Added Net Weighted Amount" = Table.AddColumn(#"Added Weighted Amount", "Net_Weighted_Legacy_Amount", each 
        [Net_Legacy_Amount] * [Oppo_Certainty], type number),
    #"Reordered Columns" = Table.ReorderColumns(#"Added Net Weighted Amount",{"Oppo_OpportunityId", "Oppo_PrimaryCompanyId", "Oppo_PrimaryPersonId", "Oppo_AssignedUserId", "Oppo_ChannelId", "Oppo_Description", "Oppo_Type", "Oppo_Product", "Oppo_Source", "Oppo_Note", "Oppo_CustomerRef", "Oppo_Opened", "Oppo_Closed", "Oppo_Status", "Oppo_Stage", "Legacy_Amount", "Oppo_Certainty", "Oppo_Priority", "Oppo_TargetClose", "Oppo_CreatedBy", "Oppo_CreatedDate", "Oppo_UpdatedBy", "Oppo_UpdatedDate", "Oppo_TimeStamp", "Oppo_Deleted", "Oppo_Total", "Oppo_NotifyTime", "Oppo_SMSSent", "Oppo_WaveItemId", "Oppo_SecTerr", "Oppo_WorkflowId", "Oppo_LeadID", "Oppo_Forecast_CID", "Oppo_Total_CID", "oppo_scenario", "oppo_decisiontimeframe", "oppo_Currency", "oppo_TotalOrders_CID", "oppo_TotalOrders", "oppo_totalQuotes_CID", "oppo_totalQuotes", "oppo_NoDiscAmtSum", "oppo_NoDiscAmtSum_CID", "Oppo_PrimaryAccountId", "oppo_SCRMcompetitor", "oppo_SCRMwinner", "oppo_SCRMreasonforloss", "Oppo_SCRMIsCrossSell", "Oppo_SCRMOriginalOppoId", "oppo_TalendExterKey", "oppo_obfuscated", "oppo_note_qualif", "oppo_Date_Q_Qualif", "oppo_cout", "oppo_cout_CID", "Net Amount", "Weighted Amount", "Net Weighted Amount", "Record ID", "Deal Name", "Deal Stage", "Predicted_CRM_Deal_Stage", "Pipeline", "icalps_company_id", "icalps_contact_id", "icalps_deal_id", "Legacy_Mapped_Stage", "Legacy_Mapped_Status", "Pipeline_Type", "OppoStage", "Stage_Match", "Net_Legacy_Amount", "Weighted_Legacy_Amount", "Net_Weighted_Legacy_Amount"}),
    #"Removed Columns" = Table.RemoveColumns(#"Reordered Columns",{"Oppo_TargetClose", "Oppo_CreatedBy", "Oppo_CreatedDate", "Oppo_UpdatedBy", "Oppo_UpdatedDate", "Oppo_TimeStamp", "Oppo_Deleted", "Oppo_Total", "Oppo_NotifyTime", "Oppo_SMSSent", "Oppo_WaveItemId", "Oppo_SecTerr", "Oppo_WorkflowId", "Oppo_LeadID", "Oppo_Forecast_CID", "Oppo_Total_CID", "oppo_scenario", "oppo_decisiontimeframe", "oppo_Currency", "oppo_TotalOrders_CID", "oppo_TotalOrders", "oppo_totalQuotes_CID", "oppo_totalQuotes", "oppo_NoDiscAmtSum", "oppo_NoDiscAmtSum_CID", "Oppo_PrimaryAccountId", "oppo_SCRMcompetitor", "oppo_SCRMwinner", "oppo_SCRMreasonforloss", "Oppo_SCRMIsCrossSell", "Oppo_SCRMOriginalOppoId", "oppo_TalendExterKey", "oppo_obfuscated", "oppo_note_qualif", "oppo_Date_Q_Qualif", "Net Amount", "Weighted Amount", "Net Weighted Amount"}),
    #"Renamed Columns" = Table.RenameColumns(#"Removed Columns",{{"OppoStage", "IcAlpsStageRef"}}),
    #"Removed Columns1" = Table.RemoveColumns(#"Renamed Columns",{"Pipeline_Type"}),
    #"Renamed Columns1" = Table.RenameColumns(#"Removed Columns1",{{"Legacy_Mapped_Status", "IcAlps_Status"}, {"Legacy_Mapped_Stage", "IcAlps_Stage"}, {"Net_Legacy_Amount", "Amount"}, {"Weighted_Legacy_Amount", "Weighted_Amount"}, {"Net_Weighted_Legacy_Amount", "Net_Weighted_Amount"}}),
    #"Reordered Columns1" = Table.ReorderColumns(#"Renamed Columns1",{"Oppo_OpportunityId", "Oppo_PrimaryCompanyId", "Oppo_PrimaryPersonId", "Oppo_AssignedUserId", "Oppo_ChannelId", "Oppo_Description", "Oppo_Type", "Oppo_Product", "Oppo_Source", "Oppo_Note", "Oppo_CustomerRef", "Oppo_Opened", "Oppo_Closed", "Oppo_Status", "Oppo_Stage", "Legacy_Amount", "Oppo_Priority", "oppo_cout", "oppo_cout_CID", "Record ID", "Deal Name", "Deal Stage", "Predicted_CRM_Deal_Stage", "Pipeline", "icalps_company_id", "icalps_contact_id", "icalps_deal_id", "IcAlps_Stage", "IcAlps_Status", "IcAlpsStageRef", "Stage_Match", "Oppo_Certainty", "Amount", "Weighted_Amount", "Net_Weighted_Amount"}),
    #"Divided Column" = Table.TransformColumns(#"Reordered Columns1", {{"Oppo_Certainty", each _ / 100, type number}}),
    #"Inserted Multiplication" = Table.AddColumn(#"Divided Column", "Multiplication", each [Amount] * [Oppo_Certainty], type number),
    #"Removed Columns2" = Table.RemoveColumns(#"Inserted Multiplication",{"Weighted_Amount"}),
    #"Renamed Columns2" = Table.RenameColumns(#"Removed Columns2",{{"Multiplication", "Weighted_Amount"}}),
    #"Reordered Columns2" = Table.ReorderColumns(#"Renamed Columns2",{"Oppo_OpportunityId", "Oppo_PrimaryCompanyId", "Oppo_PrimaryPersonId", "Oppo_AssignedUserId", "Oppo_ChannelId", "Oppo_Description", "Oppo_Type", "Oppo_Product", "Oppo_Source", "Oppo_Note", "Oppo_CustomerRef", "Oppo_Opened", "Oppo_Closed", "Oppo_Status", "Oppo_Stage", "Legacy_Amount", "Oppo_Priority", "oppo_cout_CID", "Record ID", "Deal Name", "Deal Stage", "Predicted_CRM_Deal_Stage", "Pipeline", "icalps_company_id", "icalps_contact_id", "icalps_deal_id", "IcAlps_Stage", "IcAlps_Status", "IcAlpsStageRef", "Stage_Match", "Oppo_Certainty", "oppo_cout", "Amount", "Net_Weighted_Amount", "Weighted_Amount"}),
    #"Inserted Subtraction" = Table.AddColumn(#"Reordered Columns2", "Subtraction", each [Amount] - [oppo_cout], type number),
    #"Renamed Columns3" = Table.RenameColumns(#"Inserted Subtraction",{{"Subtraction", "Net Amount"}, {"Stage_Match", "IcAlpsStage_Match"}}),
    #"Filtered Rows" = Table.SelectRows(#"Renamed Columns3", each ([Record ID] <> null))

in
    #"Filtered Rows"