# Top-level keys that must be present and non-empty on every record, per vendor.
# Confirmed against each vendor's official Orders API docs -- see README "Normalized Order Model".
REQUIRED_FIELDS = {
    "shopify": ("id", "created_at", "total_price"),  # REST Admin API Order object
    "toast": ("guid", "businessDate"),  # Orders API v2 Order object
    "clover": ("id", "createdTime"),  # REST API v3 Order object
}

# Full top-level field baseline per vendor, as of the last time this file was checked against the
# vendor's official docs (see README "Normalized Order Model"). Used only for schema-drift
# detection -- a field appearing on a record that isn't in this set means the vendor likely added
# a new column since this baseline was captured. Not a validation gate: unknown fields don't get
# rejected, just flagged so a human updates this list (and the Normalized Order Model) deliberately.
KNOWN_FIELDS = {
    "shopify": {
        "id", "admin_graphql_api_id", "app_id", "billing_address", "browser_ip",
        "buyer_accepts_marketing", "cancel_reason", "cancelled_at", "cart_token", "checkout_id",
        "checkout_token", "client_details", "closed_at", "confirmation_number", "confirmed",
        "contact_email", "created_at", "currency", "current_subtotal_price",
        "current_total_discounts", "current_total_price", "current_total_tax", "customer",
        "discount_applications", "discount_codes", "email", "financial_status",
        "fulfillment_status", "fulfillments", "gateway", "landing_site", "line_items",
        "location_id", "name", "note", "note_attributes", "number", "order_number",
        "order_status_url", "payment_gateway_names", "phone", "presentment_currency",
        "processed_at", "processing_method", "referring_site", "refunds", "shipping_address",
        "shipping_lines", "source_name", "subtotal_price", "tags", "tax_lines", "taxes_included",
        "total_discounts", "total_line_items_price", "total_outstanding", "total_price",
        "total_tax", "total_tip_received", "total_weight", "updated_at", "user_id",
    },
    "toast": {
        "guid", "entityType", "externalId", "businessDate", "revisionNumber", "source",
        "duration", "deliveryInfo", "curbsidePickupInfo", "openedDate", "voidDate", "voided",
        "voidBusinessDate", "paidDate", "closedDate", "deletedDate", "deleted", "promisedDate",
        "channelGuid", "diningOption", "checks", "table", "serviceArea", "restaurantService",
        "revenueCenter", "server", "lastModifiedDevice", "createdDevice", "createdDate",
        "modifiedDate", "createdByClientName", "createdInTestMode", "initialCreatedDate",
        "estimatedFulfillmentDate", "numberOfGuests", "guestOrderId", "approvalStatus",
        "excessFood", "marketplaceFacilitatorTaxInfo",
    },
    "clover": {
        "id", "currency", "employee", "total", "externalReferenceId", "unpaidBalance",
        "paymentState", "title", "note", "orderType", "taxRemoved", "isVat", "manualTransaction",
        "groupLineItems", "testMode", "state", "payType", "createdTime", "clientCreatedTime",
        "modifiedTime", "deletedTimestamp", "serviceCharge", "additionalCharges", "discounts",
        "lineItems", "payments", "refunds", "credits", "voids", "preAuths", "authorizations",
        "printGroups", "device", "merchant", "orderFulfillmentEvent", "customers",
    },
}


# Required fields that carry a monetary amount or an epoch-based timestamp, per vendor. These are
# checked for type/range on top of presence -- a value that's present but the wrong shape (e.g. a
# negative amount, or a non-numeric epoch) would otherwise sail through as "valid" and silently
# corrupt totals once the value reaches the curated layer.
NUMERIC_FIELD_CHECKS = {
    "shopify": ("total_price",),  # string decimal, e.g. "19.99"
    "toast": ("businessDate",),  # int, YYYYMMDD
    "clover": ("total", "createdTime"),  # int cents / epoch ms
}


def _is_non_negative_number(value) -> bool:
    try:
        return float(value) >= 0
    except (TypeError, ValueError):
        return False


def _is_valid_business_date(value) -> bool:
    try:
        value = int(value)
    except (TypeError, ValueError):
        return False
    if value < 10000101 or value > 99991231:
        return False
    month, day = (value // 100) % 100, value % 100
    return 1 <= month <= 12 and 1 <= day <= 31


def validate_record(record: dict, vendor: str):
    """Return None if the record is valid, else a short reason string.
    Checks presence/non-emptiness of required fields, plus type/range on the subset of those
    fields that carry a monetary amount or epoch timestamp (see NUMERIC_FIELD_CHECKS). Still no
    referential checks."""
    for field in REQUIRED_FIELDS.get(vendor, ()):
        if record.get(field) in (None, ""):
            return f"missing required field: {field}"

    for field in NUMERIC_FIELD_CHECKS.get(vendor, ()):
        value = record.get(field)
        if value in (None, ""):
            continue  # already covered by the required-field check above if applicable
        if field == "businessDate":
            if not _is_valid_business_date(value):
                return f"invalid value for field {field}: expected YYYYMMDD, got {value!r}"
        elif not _is_non_negative_number(value):
            return f"invalid value for field {field}: expected a non-negative number, got {value!r}"

    return None


def detect_new_fields(record: dict, vendor: str) -> set:
    """Top-level keys on this record that aren't in KNOWN_FIELDS -- signals the vendor may have
    added a field since the baseline was last captured. Returns an empty set for unknown vendors."""
    known = KNOWN_FIELDS.get(vendor)
    if known is None:
        return set()
    return set(record.keys()) - known
