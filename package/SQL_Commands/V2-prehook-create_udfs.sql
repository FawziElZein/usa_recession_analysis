CREATE OR REPLACE FUNCTION target_schema.get_state_name(state_initial VARCHAR(2))
RETURNS VARCHAR(20) AS $$
BEGIN
    CASE state_initial
        WHEN 'AL' THEN RETURN 'Alabama';
        WHEN 'AK' THEN RETURN 'Alaska';
        WHEN 'AZ' THEN RETURN 'Arizona';
        WHEN 'AR' THEN RETURN 'Arkansas';
        WHEN 'CA' THEN RETURN 'California';
        WHEN 'CO' THEN RETURN 'Colorado';
        WHEN 'CT' THEN RETURN 'Connecticut';
        WHEN 'DE' THEN RETURN 'Delaware';
        WHEN 'FL' THEN RETURN 'Florida';
        WHEN 'GA' THEN RETURN 'Georgia';
        WHEN 'HI' THEN RETURN 'Hawaii';
        WHEN 'ID' THEN RETURN 'Idaho';
        WHEN 'IL' THEN RETURN 'Illinois';
        WHEN 'IN' THEN RETURN 'Indiana';
        WHEN 'IA' THEN RETURN 'Iowa';
        WHEN 'KS' THEN RETURN 'Kansas';
        WHEN 'KY' THEN RETURN 'Kentucky';
        WHEN 'LA' THEN RETURN 'Louisiana';
        WHEN 'ME' THEN RETURN 'Maine';
        WHEN 'MD' THEN RETURN 'Maryland';
        WHEN 'MA' THEN RETURN 'Massachusetts';
        WHEN 'MI' THEN RETURN 'Michigan';
        WHEN 'MN' THEN RETURN 'Minnesota';
        WHEN 'MS' THEN RETURN 'Mississippi';
        WHEN 'MO' THEN RETURN 'Missouri';
        WHEN 'MT' THEN RETURN 'Montana';
        WHEN 'NE' THEN RETURN 'Nebraska';
        WHEN 'NV' THEN RETURN 'Nevada';
        WHEN 'NH' THEN RETURN 'New Hampshire';
        WHEN 'NJ' THEN RETURN 'New Jersey';
        WHEN 'NM' THEN RETURN 'New Mexico';
        WHEN 'NY' THEN RETURN 'New York';
        WHEN 'NC' THEN RETURN 'North Carolina';
        WHEN 'ND' THEN RETURN 'North Dakota';
        WHEN 'OH' THEN RETURN 'Ohio';
        WHEN 'OK' THEN RETURN 'Oklahoma';
        WHEN 'OR' THEN RETURN 'Oregon';
        WHEN 'PA' THEN RETURN 'Pennsylvania';
        WHEN 'RI' THEN RETURN 'Rhode Island';
        WHEN 'SC' THEN RETURN 'South Carolina';
        WHEN 'SD' THEN RETURN 'South Dakota';
        WHEN 'TN' THEN RETURN 'Tennessee';
        WHEN 'TX' THEN RETURN 'Texas';
        WHEN 'UT' THEN RETURN 'Utah';
        WHEN 'VT' THEN RETURN 'Vermont';
        WHEN 'VA' THEN RETURN 'Virginia';
        WHEN 'WA' THEN RETURN 'Washington';
        WHEN 'WV' THEN RETURN 'West Virginia';
        WHEN 'WI' THEN RETURN 'Wisconsin';
        WHEN 'WY' THEN RETURN 'Wyoming';
        ELSE RETURN NULL;
    END CASE;
END;
$$ LANGUAGE plpgsql;


