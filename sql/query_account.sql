/**
 * Stored procedure script supporting account queries from the AccountsDb plugin
 */


CREATE OR REPLACE FUNCTION get_account_with_commitment_level(
    pubkey BYTEA,
    commitment_level VARCHAR(16)
)
RETURNS SETOF account
LANGUAGE plpgsql
AS $$
BEGIN

    SELECT * FROM account AS acct
    JOIN slot AS s ON acct.slot = s.slot
    WHERE acct.pubkey = $1
    AND s.status = $2;
    EXCEPTION
        WHEN no_data_found THEN BEGIN
            SELECT * FROM account_audit AS acct1
            JOIN slot AS s ON acct1.slot = s.slot
            WHERE acct.pubkey = $1
            AND s.status = $2;
            WHERE NOT EXISTS (
                SELECT NULL FROM account_audit AS acct2
                JOIN slot AS s2 ON acct2.slot = s2.slot
                WHERE s.slot < s2.slot
                AND acct2.pubkey = $1
                AND s2.status = $2
            );
        END;
END; $$

