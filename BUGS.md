# Known Bugs

## BUG-001: Parameter order mismatch in original functions
**File:** `local/aws_quotas.py`
**Severity:** High — threshold events will never fire for affected quotas
**Functions affected:**
- `L_DC2B2D3D` (S3 buckets)
- `L_0DA4ABF3` (IAM managed policies per role)
- `L_BF35879D` (IAM server certificates)

**Problem:** These functions are defined with `(serviceCode, quotaCode, region, threshold)` instead of the standard `(serviceCode, quotaCode, threshold, region)`. Since `app.py` calls all functions with keyword args, the values land correctly at the call site — but inside the functions the local variable names are swapped, so `threshold` holds the region string and `region` holds the numeric threshold. This means threshold comparisons like `float(threshold)/100` will raise a `ValueError` at runtime.

**Note:** `L_CD17FD4B` and `L_6408ABDE` were previously listed here in error — they both use the correct `(serviceCode, quotaCode, threshold, region)` signature.

---

## BUG-002: Parameter order mismatch in new IAM functions (merged from Downloads branch)
**File:** `local/aws_quotas.py`
**Severity:** High — same issue as BUG-001
**Functions affected (all new IAM functions):**
- `L_F55AF5E4`, `L_FC9EC213`, `L_C07B4B0D`, `L_3AD47CAE`, `L_F4A5425F`
- `L_8E23FFD8`, `L_ED111B8C`, `L_6E65F664`, `L_4019AD8B`, `L_B39FB15B`
- `L_FE177D64`, `L_F1176D35`, `L_C4DF001E`, `L_E95E4862`, `L_DB618D39`
- `L_384571C4`, `L_8758042E`, `L_7A1621EC`, `L_858F3967`, `L_19F2CF71`, `L_76C48054`

**Problem:** All new IAM functions use `(serviceCode, quotaCode, region, threshold)` — same wrong order as BUG-001. The swapped local variable names cause `float(threshold)/100` to receive the region string, raising a `ValueError` at runtime.

---

## BUG-003: ~~IAM boto3 clients created without region_name~~ (FIXED)
**File:** `local/aws_quotas.py`
**Severity:** Medium — will fail if no default region is configured in the environment
**Functions affected:**
- `L_0DA4ABF3`, `L_BF35879D` (original functions)
- All 21 new IAM functions listed in BUG-002

**Problem:** `boto3.client('iam')` and `boto3.client('service-quotas')` are created without `region_name=region`. IAM is a global service so it doesn't need a region, but `service-quotas` does. This will throw `NoRegionError` if `AWS_DEFAULT_REGION` is not set in the environment.

**Note:** Due to BUG-001/BUG-002, the `region` variable inside these functions actually holds the threshold value (a number), not the region string. BUG-001/BUG-002 must be fixed first before this fix will work correctly.

**Fix:** Change `boto3.client('service-quotas')` to `boto3.client('service-quotas', region_name=region)` in all affected functions.

---

## BUG-004: ~~Missing parameters in L_CE3125E5 updateQuotaUsage call~~ (FIXED)
**File:** `local/aws_quotas.py`
**Severity:** High — threshold alerts will never fire for ELB registered instances quota
**Function:** `L_CE3125E5`

**Problem:** The `updateQuotaUsage()` call at the end of this function is missing the last two parameters (`resourceListCrossingThreshold` and `sendQuotaThresholdEvent`). The function correctly calculates `usage_percentage` and logs a warning when the threshold is exceeded, but never sets `sendQuotaThresholdEvent = True` and never passes it (or `resourceListCrossingThreshold`) to `updateQuotaUsage`. Threshold events are therefore never sent for this quota.

**Fix:** Initialize `resourceListCrossingThreshold = []` and `sendQuotaThresholdEvent = False` at the top of the function, populate them in the threshold check block, and pass both to `updateQuotaUsage`.
