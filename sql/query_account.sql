/**
 * Stored procedure script supporting account queries from the AccountsDb plugin
 */


CREATE OR REPLACE FUNCTION get_account_with_commitment_level(
    input_pubkey BYTEA,
    commitment_level VARCHAR(16)
)
RETURNS RECORD
LANGUAGE plpgsql
AS
$$
DECLARE
ret RECORD;
BEGIN

    SELECT acct.* FROM account AS acct
    JOIN slot AS s ON acct.slot = s.slot
    WHERE acct.pubkey = input_pubkey
    AND s.status = commitment_level INTO ret;
    EXCEPTION
        WHEN no_data_found THEN BEGIN
            SELECT * FROM account_audit AS acct1
            JOIN slot AS s ON acct1.slot = s.slot
            WHERE acct1.pubkey = input_pubkey
            AND s.status = commitment_level
            AND NOT EXISTS (
                SELECT NULL FROM account_audit AS acct2
                JOIN slot AS s2 ON acct2.slot = s2.slot
                WHERE s.slot < s2.slot
                AND acct2.pubkey = input_pubkey
                AND s2.status = commitment_level
            )
            INTO ret;
        END;
    RETURN ret;
END;

$$;

