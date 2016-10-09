/**
 * Autogenerated by Avro
 *
 * DO NOT EDIT DIRECTLY
 */
package com.refactorlabs.cs378.assign6;

import org.apache.avro.specific.SpecificData;

@SuppressWarnings("all")
@org.apache.avro.specific.AvroGenerated
public class VinImpressionCounts extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  private static final long serialVersionUID = 9131315114487213926L;
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"VinImpressionCounts\",\"namespace\":\"com.refactorlabs.cs378.assign6\",\"fields\":[{\"name\":\"unique_users\",\"type\":\"long\",\"default\":0},{\"name\":\"clicks\",\"type\":[\"null\",{\"type\":\"map\",\"values\":\"long\"}],\"default\":null},{\"name\":\"edit_contact_form\",\"type\":\"long\",\"default\":0},{\"name\":\"marketplace_srps\",\"type\":\"long\",\"default\":0},{\"name\":\"marketplace_vdps\",\"type\":\"long\",\"default\":0}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }
  @Deprecated public long unique_users;
  @Deprecated public java.util.Map<java.lang.CharSequence,java.lang.Long> clicks;
  @Deprecated public long edit_contact_form;
  @Deprecated public long marketplace_srps;
  @Deprecated public long marketplace_vdps;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>.
   */
  public VinImpressionCounts() {}

  /**
   * All-args constructor.
   * @param unique_users The new value for unique_users
   * @param clicks The new value for clicks
   * @param edit_contact_form The new value for edit_contact_form
   * @param marketplace_srps The new value for marketplace_srps
   * @param marketplace_vdps The new value for marketplace_vdps
   */
  public VinImpressionCounts(java.lang.Long unique_users, java.util.Map<java.lang.CharSequence,java.lang.Long> clicks, java.lang.Long edit_contact_form, java.lang.Long marketplace_srps, java.lang.Long marketplace_vdps) {
    this.unique_users = unique_users;
    this.clicks = clicks;
    this.edit_contact_form = edit_contact_form;
    this.marketplace_srps = marketplace_srps;
    this.marketplace_vdps = marketplace_vdps;
  }

  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call.
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return unique_users;
    case 1: return clicks;
    case 2: return edit_contact_form;
    case 3: return marketplace_srps;
    case 4: return marketplace_vdps;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  // Used by DatumReader.  Applications should not call.
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: unique_users = (java.lang.Long)value$; break;
    case 1: clicks = (java.util.Map<java.lang.CharSequence,java.lang.Long>)value$; break;
    case 2: edit_contact_form = (java.lang.Long)value$; break;
    case 3: marketplace_srps = (java.lang.Long)value$; break;
    case 4: marketplace_vdps = (java.lang.Long)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'unique_users' field.
   * @return The value of the 'unique_users' field.
   */
  public java.lang.Long getUniqueUsers() {
    return unique_users;
  }

  /**
   * Sets the value of the 'unique_users' field.
   * @param value the value to set.
   */
  public void setUniqueUsers(java.lang.Long value) {
    this.unique_users = value;
  }

  /**
   * Gets the value of the 'clicks' field.
   * @return The value of the 'clicks' field.
   */
  public java.util.Map<java.lang.CharSequence,java.lang.Long> getClicks() {
    return clicks;
  }

  /**
   * Sets the value of the 'clicks' field.
   * @param value the value to set.
   */
  public void setClicks(java.util.Map<java.lang.CharSequence,java.lang.Long> value) {
    this.clicks = value;
  }

  /**
   * Gets the value of the 'edit_contact_form' field.
   * @return The value of the 'edit_contact_form' field.
   */
  public java.lang.Long getEditContactForm() {
    return edit_contact_form;
  }

  /**
   * Sets the value of the 'edit_contact_form' field.
   * @param value the value to set.
   */
  public void setEditContactForm(java.lang.Long value) {
    this.edit_contact_form = value;
  }

  /**
   * Gets the value of the 'marketplace_srps' field.
   * @return The value of the 'marketplace_srps' field.
   */
  public java.lang.Long getMarketplaceSrps() {
    return marketplace_srps;
  }

  /**
   * Sets the value of the 'marketplace_srps' field.
   * @param value the value to set.
   */
  public void setMarketplaceSrps(java.lang.Long value) {
    this.marketplace_srps = value;
  }

  /**
   * Gets the value of the 'marketplace_vdps' field.
   * @return The value of the 'marketplace_vdps' field.
   */
  public java.lang.Long getMarketplaceVdps() {
    return marketplace_vdps;
  }

  /**
   * Sets the value of the 'marketplace_vdps' field.
   * @param value the value to set.
   */
  public void setMarketplaceVdps(java.lang.Long value) {
    this.marketplace_vdps = value;
  }

  /**
   * Creates a new VinImpressionCounts RecordBuilder.
   * @return A new VinImpressionCounts RecordBuilder
   */
  public static com.refactorlabs.cs378.assign6.VinImpressionCounts.Builder newBuilder() {
    return new com.refactorlabs.cs378.assign6.VinImpressionCounts.Builder();
  }

  /**
   * Creates a new VinImpressionCounts RecordBuilder by copying an existing Builder.
   * @param other The existing builder to copy.
   * @return A new VinImpressionCounts RecordBuilder
   */
  public static com.refactorlabs.cs378.assign6.VinImpressionCounts.Builder newBuilder(com.refactorlabs.cs378.assign6.VinImpressionCounts.Builder other) {
    return new com.refactorlabs.cs378.assign6.VinImpressionCounts.Builder(other);
  }

  /**
   * Creates a new VinImpressionCounts RecordBuilder by copying an existing VinImpressionCounts instance.
   * @param other The existing instance to copy.
   * @return A new VinImpressionCounts RecordBuilder
   */
  public static com.refactorlabs.cs378.assign6.VinImpressionCounts.Builder newBuilder(com.refactorlabs.cs378.assign6.VinImpressionCounts other) {
    return new com.refactorlabs.cs378.assign6.VinImpressionCounts.Builder(other);
  }

  /**
   * RecordBuilder for VinImpressionCounts instances.
   */
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<VinImpressionCounts>
    implements org.apache.avro.data.RecordBuilder<VinImpressionCounts> {

    private long unique_users;
    private java.util.Map<java.lang.CharSequence,java.lang.Long> clicks;
    private long edit_contact_form;
    private long marketplace_srps;
    private long marketplace_vdps;

    /** Creates a new Builder */
    private Builder() {
      super(SCHEMA$);
    }

    /**
     * Creates a Builder by copying an existing Builder.
     * @param other The existing Builder to copy.
     */
    private Builder(com.refactorlabs.cs378.assign6.VinImpressionCounts.Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.unique_users)) {
        this.unique_users = data().deepCopy(fields()[0].schema(), other.unique_users);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.clicks)) {
        this.clicks = data().deepCopy(fields()[1].schema(), other.clicks);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.edit_contact_form)) {
        this.edit_contact_form = data().deepCopy(fields()[2].schema(), other.edit_contact_form);
        fieldSetFlags()[2] = true;
      }
      if (isValidValue(fields()[3], other.marketplace_srps)) {
        this.marketplace_srps = data().deepCopy(fields()[3].schema(), other.marketplace_srps);
        fieldSetFlags()[3] = true;
      }
      if (isValidValue(fields()[4], other.marketplace_vdps)) {
        this.marketplace_vdps = data().deepCopy(fields()[4].schema(), other.marketplace_vdps);
        fieldSetFlags()[4] = true;
      }
    }

    /**
     * Creates a Builder by copying an existing VinImpressionCounts instance
     * @param other The existing instance to copy.
     */
    private Builder(com.refactorlabs.cs378.assign6.VinImpressionCounts other) {
            super(SCHEMA$);
      if (isValidValue(fields()[0], other.unique_users)) {
        this.unique_users = data().deepCopy(fields()[0].schema(), other.unique_users);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.clicks)) {
        this.clicks = data().deepCopy(fields()[1].schema(), other.clicks);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.edit_contact_form)) {
        this.edit_contact_form = data().deepCopy(fields()[2].schema(), other.edit_contact_form);
        fieldSetFlags()[2] = true;
      }
      if (isValidValue(fields()[3], other.marketplace_srps)) {
        this.marketplace_srps = data().deepCopy(fields()[3].schema(), other.marketplace_srps);
        fieldSetFlags()[3] = true;
      }
      if (isValidValue(fields()[4], other.marketplace_vdps)) {
        this.marketplace_vdps = data().deepCopy(fields()[4].schema(), other.marketplace_vdps);
        fieldSetFlags()[4] = true;
      }
    }

    /**
      * Gets the value of the 'unique_users' field.
      * @return The value.
      */
    public java.lang.Long getUniqueUsers() {
      return unique_users;
    }

    /**
      * Sets the value of the 'unique_users' field.
      * @param value The value of 'unique_users'.
      * @return This builder.
      */
    public com.refactorlabs.cs378.assign6.VinImpressionCounts.Builder setUniqueUsers(long value) {
      validate(fields()[0], value);
      this.unique_users = value;
      fieldSetFlags()[0] = true;
      return this;
    }

    /**
      * Checks whether the 'unique_users' field has been set.
      * @return True if the 'unique_users' field has been set, false otherwise.
      */
    public boolean hasUniqueUsers() {
      return fieldSetFlags()[0];
    }


    /**
      * Clears the value of the 'unique_users' field.
      * @return This builder.
      */
    public com.refactorlabs.cs378.assign6.VinImpressionCounts.Builder clearUniqueUsers() {
      fieldSetFlags()[0] = false;
      return this;
    }

    /**
      * Gets the value of the 'clicks' field.
      * @return The value.
      */
    public java.util.Map<java.lang.CharSequence,java.lang.Long> getClicks() {
      return clicks;
    }

    /**
      * Sets the value of the 'clicks' field.
      * @param value The value of 'clicks'.
      * @return This builder.
      */
    public com.refactorlabs.cs378.assign6.VinImpressionCounts.Builder setClicks(java.util.Map<java.lang.CharSequence,java.lang.Long> value) {
      validate(fields()[1], value);
      this.clicks = value;
      fieldSetFlags()[1] = true;
      return this;
    }

    /**
      * Checks whether the 'clicks' field has been set.
      * @return True if the 'clicks' field has been set, false otherwise.
      */
    public boolean hasClicks() {
      return fieldSetFlags()[1];
    }


    /**
      * Clears the value of the 'clicks' field.
      * @return This builder.
      */
    public com.refactorlabs.cs378.assign6.VinImpressionCounts.Builder clearClicks() {
      clicks = null;
      fieldSetFlags()[1] = false;
      return this;
    }

    /**
      * Gets the value of the 'edit_contact_form' field.
      * @return The value.
      */
    public java.lang.Long getEditContactForm() {
      return edit_contact_form;
    }

    /**
      * Sets the value of the 'edit_contact_form' field.
      * @param value The value of 'edit_contact_form'.
      * @return This builder.
      */
    public com.refactorlabs.cs378.assign6.VinImpressionCounts.Builder setEditContactForm(long value) {
      validate(fields()[2], value);
      this.edit_contact_form = value;
      fieldSetFlags()[2] = true;
      return this;
    }

    /**
      * Checks whether the 'edit_contact_form' field has been set.
      * @return True if the 'edit_contact_form' field has been set, false otherwise.
      */
    public boolean hasEditContactForm() {
      return fieldSetFlags()[2];
    }


    /**
      * Clears the value of the 'edit_contact_form' field.
      * @return This builder.
      */
    public com.refactorlabs.cs378.assign6.VinImpressionCounts.Builder clearEditContactForm() {
      fieldSetFlags()[2] = false;
      return this;
    }

    /**
      * Gets the value of the 'marketplace_srps' field.
      * @return The value.
      */
    public java.lang.Long getMarketplaceSrps() {
      return marketplace_srps;
    }

    /**
      * Sets the value of the 'marketplace_srps' field.
      * @param value The value of 'marketplace_srps'.
      * @return This builder.
      */
    public com.refactorlabs.cs378.assign6.VinImpressionCounts.Builder setMarketplaceSrps(long value) {
      validate(fields()[3], value);
      this.marketplace_srps = value;
      fieldSetFlags()[3] = true;
      return this;
    }

    /**
      * Checks whether the 'marketplace_srps' field has been set.
      * @return True if the 'marketplace_srps' field has been set, false otherwise.
      */
    public boolean hasMarketplaceSrps() {
      return fieldSetFlags()[3];
    }


    /**
      * Clears the value of the 'marketplace_srps' field.
      * @return This builder.
      */
    public com.refactorlabs.cs378.assign6.VinImpressionCounts.Builder clearMarketplaceSrps() {
      fieldSetFlags()[3] = false;
      return this;
    }

    /**
      * Gets the value of the 'marketplace_vdps' field.
      * @return The value.
      */
    public java.lang.Long getMarketplaceVdps() {
      return marketplace_vdps;
    }

    /**
      * Sets the value of the 'marketplace_vdps' field.
      * @param value The value of 'marketplace_vdps'.
      * @return This builder.
      */
    public com.refactorlabs.cs378.assign6.VinImpressionCounts.Builder setMarketplaceVdps(long value) {
      validate(fields()[4], value);
      this.marketplace_vdps = value;
      fieldSetFlags()[4] = true;
      return this;
    }

    /**
      * Checks whether the 'marketplace_vdps' field has been set.
      * @return True if the 'marketplace_vdps' field has been set, false otherwise.
      */
    public boolean hasMarketplaceVdps() {
      return fieldSetFlags()[4];
    }


    /**
      * Clears the value of the 'marketplace_vdps' field.
      * @return This builder.
      */
    public com.refactorlabs.cs378.assign6.VinImpressionCounts.Builder clearMarketplaceVdps() {
      fieldSetFlags()[4] = false;
      return this;
    }

    @Override
    public VinImpressionCounts build() {
      try {
        VinImpressionCounts record = new VinImpressionCounts();
        record.unique_users = fieldSetFlags()[0] ? this.unique_users : (java.lang.Long) defaultValue(fields()[0]);
        record.clicks = fieldSetFlags()[1] ? this.clicks : (java.util.Map<java.lang.CharSequence,java.lang.Long>) defaultValue(fields()[1]);
        record.edit_contact_form = fieldSetFlags()[2] ? this.edit_contact_form : (java.lang.Long) defaultValue(fields()[2]);
        record.marketplace_srps = fieldSetFlags()[3] ? this.marketplace_srps : (java.lang.Long) defaultValue(fields()[3]);
        record.marketplace_vdps = fieldSetFlags()[4] ? this.marketplace_vdps : (java.lang.Long) defaultValue(fields()[4]);
        return record;
      } catch (Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }

  private static final org.apache.avro.io.DatumWriter
    WRITER$ = new org.apache.avro.specific.SpecificDatumWriter(SCHEMA$);

  @Override public void writeExternal(java.io.ObjectOutput out)
    throws java.io.IOException {
    WRITER$.write(this, SpecificData.getEncoder(out));
  }

  private static final org.apache.avro.io.DatumReader
    READER$ = new org.apache.avro.specific.SpecificDatumReader(SCHEMA$);

  @Override public void readExternal(java.io.ObjectInput in)
    throws java.io.IOException {
    READER$.read(this, SpecificData.getDecoder(in));
  }

}
