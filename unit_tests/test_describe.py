from source_zuora.zuora_describe import (
    foreign_key_columns,
    parse_fields,
    parse_object_names,
    parse_relationships,
)

# Shape taken verbatim from GET /v1/describe on an APAC sandbox.
OBJECTS_XML = b"""<?xml version="1.0" encoding="UTF-8"?>
<objects>
    <object href="https://x/describe/Account">
     <name>Account</name>
     <label>Account</label>
</object>
    <object href="https://x/describe/Subscription">
     <name>Subscription</name>
     <label>Subscription</label>
</object>
</objects>
"""

# Shape taken verbatim from GET /v1/describe/Subscription. Note `Notes` is
# soap-only (no export context) and must be dropped.
SUBSCRIPTION_XML = b"""<?xml version="1.0" encoding="UTF-8"?>
<object href="https://x/describe/Subscription">
   <name>Subscription</name>
   <label>Subscription</label>
   <fields>
      <field>
         <name>Id</name>
         <type>text</type>
         <contexts><context>soap</context><context>export</context></contexts>
      </field>
      <field>
         <name>SubscriptionId</name>
         <type>text</type>
         <contexts><context>export</context></contexts>
      </field>
      <field>
         <name>TermStartDate</name>
         <type>date</type>
         <contexts><context>export</context></contexts>
      </field>
      <field>
         <name>Notes</name>
         <type>text</type>
         <contexts><context>soap</context></contexts>
      </field>
      <field>
         <name>Custom__c</name>
         <type>picklist</type>
         <contexts><context>export</context></contexts>
      </field>
   </fields>
   <related-objects>
      <object href="https://x/describe/Account">
         <name>Account</name>
         <label>Account</label>
      </object>
      <object href="https://x/describe/Contact">
         <name>BillToContact</name>
         <label>Bill To</label>
      </object>
      <object href="https://x/describe/Subscription">
         <name>Subscription</name>
         <label>Subscription</label>
      </object>
   </related-objects>
</object>
"""

NO_FIELDS_XML = b"""<?xml version="1.0" encoding="UTF-8"?>
<object><name>BillingPreviewRun</name><label>x</label><fields></fields></object>
"""


def test_parse_object_names():
    assert parse_object_names(OBJECTS_XML) == ["Account", "Subscription"]


def test_parse_fields_keeps_only_export_context():
    fields = parse_fields(SUBSCRIPTION_XML)
    assert fields == {
        "Id": "text",
        "SubscriptionId": "text",
        "TermStartDate": "date",
        "Custom__c": "picklist",
    }
    assert "Notes" not in fields  # soap-only


def test_parse_fields_empty_when_no_export_fields():
    assert parse_fields(NO_FIELDS_XML) == {}


def test_parse_relationships():
    assert parse_relationships(SUBSCRIPTION_XML) == ["Account", "BillToContact", "Subscription"]


def test_parse_relationships_absent_section():
    assert parse_relationships(NO_FIELDS_XML) == []


def test_foreign_key_columns_derives_lowercase_names():
    fields = {"Id": "text", "TermStartDate": "date"}
    assert foreign_key_columns(fields, ["Account", "BillToContact"]) == {
        "Account": "accountid",
        "BillToContact": "billtocontactid",
    }


def test_foreign_key_columns_skips_collision_with_own_field():
    # Subscription owns `SubscriptionId`, so the `Subscription` relationship would
    # emit a duplicate `subscriptionid` column. Spec F11.
    fields = parse_fields(SUBSCRIPTION_XML)
    fks = foreign_key_columns(fields, parse_relationships(SUBSCRIPTION_XML))
    assert fks == {"Account": "accountid", "BillToContact": "billtocontactid"}
    assert "Subscription" not in fks
