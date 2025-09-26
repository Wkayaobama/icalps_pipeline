# IC'ALPS Pipeline Usage Guide

## 🎯 **Pipeline Modes Overview**

Your IC'ALPS pipeline now has **4 distinct modes** to handle different stages of your data transformation:

### **1. Enhanced Mode** ⭐ (Primary Pipeline)
```bash
python main_pipeline.py --mode enhanced
# or
python run_pipeline.py  # Choose option 1
```

**Purpose**: Complete business transformation with intermediary steps
**Sequence**: 
1. CSV → Bronze → DuckDB
2. 🔑 **Business Transformation** (applies deals_transformed_tohubspot logic)
3. 📞 **Communication Associations** (with transformed deals)
4. 🌐 **Social Networks Associations**
5. 🏢 **Site Aggregation** (child-to-parent company logic)

**Outputs** (`*_success.csv`):
- `deals_transformed_tohubspot_success.csv` - 635 deals with business rules
- `communications_associations_success.csv` - 72,909 communications 
- `companies_site_aggregation_success.csv` - 264 site aggregation records
- `companies_enhanced_success.csv` - 264 companies with site logic
- `contacts_enhanced_success.csv` - 1,061 contacts

---

### **2. HubSpot Mode** 🎯 (Final Transformation)
```bash
python main_pipeline.py --mode hubspot
# or
python hubspot_transform.py  # Standalone
```

**Purpose**: Final transformation with HubSpot MCP validation
**Prerequisites**: Enhanced mode must be run first
**Features**:
- ✅ Pipeline names validated against actual HubSpot
- ✅ Stage IDs mapped to real HubSpot stage IDs
- ✅ Property names verified via MCP server analysis
- ✅ Import batches prepared with metadata

**Outputs** (`*_import_ready.csv`):
- `hubspot_deals_import_ready.csv` - 635 deals with correct HubSpot properties
- `hubspot_companies_import_ready.csv` - Companies with HubSpot property mapping
- `hubspot_contacts_import_ready.csv` - 910 contacts with HubSpot properties  
- `hubspot_engagements_import_ready.csv` - 72,909 engagements for HubSpot
- `hubspot_import_summary.json` - Import requirements and statistics

---

### **3. Test Mode** 🧪 (Validation)
```bash
python main_pipeline.py --mode test
```

**Purpose**: Validation and testing only
**Use When**: Debugging or verifying data quality

---

### **4. Legacy Mode** 📚 (Original)
```bash
python main_pipeline.py --mode legacy
```

**Purpose**: Original pipeline behavior
**Outputs**: `enhanced_*.csv` files

---

## 🔄 **Complete Workflow**

### **Step 1: Run Enhanced Pipeline**
```bash
python main_pipeline.py --mode enhanced
```
✅ Generates business-transformed data with proper intermediary steps

### **Step 2: Run HubSpot Transformation** 
```bash
python main_pipeline.py --mode hubspot
```
✅ Creates HubSpot import-ready files with validated properties

### **Step 3: HubSpot Import** (Using MCP Server)
1. Import `hubspot_companies_import_ready.csv` 
2. Import `hubspot_contacts_import_ready.csv`
3. Import `hubspot_deals_import_ready.csv` 
4. Import `hubspot_engagements_import_ready.csv`

---

## 📊 **Key Features Implemented**

### ✅ **Business Process Rules** (Critical Intermediary Step)
- **Abandoned + Low Certainty** → `Closed Dead`
- **Pipeline Assignment**: Studies vs Sales based on `Oppo_Type`
- **Stage Transformation**: Status + Stage + Certainty logic
- **Original Values Preserved**: Audit trail maintained

### ✅ **Association Management**
- **Communication → Deal**: Links to transformed deals
- **Communication → Company/Contact**: Full entity associations
- **Social Networks → Entity**: Company vs Person mapping
- **Status Tracking**: SUCCESS/PENDING/NO_*_FOUND

### ✅ **Site Aggregation Logic**
- **Child-to-Parent**: Based on domain and name analysis
- **Multi-Site Detection**: Groups related company sites
- **Contact Aggregation**: Rolls up contacts to parent companies
- **Site Ordering**: Maintains hierarchy

### ✅ **HubSpot Compliance**
- **Actual Pipeline Names**: `Icalps_hardware`, `Icalps_service`
- **Real Stage IDs**: `1116419644`, `1162868542`, etc.
- **Verified Properties**: Based on MCP server analysis
- **Import Metadata**: Batch tracking and validation status

---

## 🎯 **Critical Pipeline Sequence (Fixed)**

**Before** (Incorrect):
```
CSV → DuckDB → Business Rules → Site Aggregation ❌
```

**After** (Correct):
```
CSV → DuckDB → Business Transformation → Communication Associations → Site Aggregation ✅
```

**Why This Matters**: 
- Business transformation must happen **before** site aggregation
- Communications must be associated with **transformed deals**, not raw opportunities
- Site aggregation works on **business-ready** company data

---

## 📁 **File Naming Conventions**

| Suffix | Purpose | Example | Pipeline Mode |
|--------|---------|---------|---------------|
| `*_success.csv` | Business transformation complete | `deals_transformed_tohubspot_success.csv` | Enhanced |
| `*_import_ready.csv` | HubSpot import prepared | `hubspot_deals_import_ready.csv` | HubSpot |
| `*_final.csv` | Legacy compatibility | `companies_with_associations_final.csv` | Legacy |

---

## 🚀 **Quick Commands**

### **Full Pipeline (Recommended)**
```bash
# Step 1: Business transformation
python main_pipeline.py --mode enhanced

# Step 2: HubSpot preparation  
python main_pipeline.py --mode hubspot
```

### **Interactive Menu**
```bash
python run_pipeline.py
```

### **Validation Only**
```bash
python validate_hubspot_properties.py
```

---

## 📋 **HubSpot Import Checklist**

### **Before Import**:
- ✅ Enhanced pipeline completed
- ✅ HubSpot transformation completed 
- ✅ Custom properties created in HubSpot
- ✅ Pipelines exist with correct names

### **Import Order**:
1. **Companies** → Get HubSpot company IDs
2. **Contacts** → Get HubSpot contact IDs
3. **Deals** → Create with associations  
4. **Engagements** → Link to all entities

### **After Import**:
- Update association status to "SUCCESS"
- Validate import counts
- Test associations in HubSpot UI

---

**🎯 Result**: Your pipeline now correctly implements the business process rules, creates proper intermediary associations, applies site aggregation logic, and generates HubSpot-ready files with validated property mappings!
