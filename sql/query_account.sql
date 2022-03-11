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

    IF NOT FOUND THEN
        SELECT acct1.* FROM account_audit AS acct1
        JOIN slot AS s ON acct1.slot = s.slot
        WHERE acct1.pubkey = input_pubkey
        AND s.status = commitment_level
        AND NOT EXISTS (
            SELECT NULL FROM account_audit AS acct2
            JOIN slot AS s2 ON acct2.slot = s2.slot
            WHERE s.slot < s2.slot
            AND acct2.pubkey = input_pubkey
            AND (commitment_level = 'processed' OR
                (commitment_level = 'confirmed' AND s2.status in ('confirmed', 'rooted')) OR
                (commitment_level = 'rooted' AND s2.status = 'rooted')
            )
        )
        INTO ret;
    END IF;
    RETURN ret;
END;

$$;


CREATE OR REPLACE FUNCTION get_account_with_commitment_level_and_slot(
    input_pubkey BYTEA,
    commitment_level VARCHAR(16),
    max_slot BIGINT
)
RETURNS SETOF account
LANGUAGE plpgsql
AS
$$
BEGIN
    RETURN QUERY
    SELECT acct.* FROM account AS acct
    JOIN slot AS s ON acct.slot = s.slot
    WHERE acct.pubkey = input_pubkey
    AND s.slot <= max_slot
    AND ( (commitment_level = 'processed' AND s.status in ('processed', 'confirmed', 'rooted')) OR 
          (commitment_level = 'confirmed' AND s.status in ('confirmed', 'rooted')) OR
          (commitment_level = 'rooted' AND s.status in ('rooted')));

    IF NOT FOUND THEN
        RETURN QUERY
        SELECT acct1.* FROM account_audit AS acct1
        JOIN slot AS s ON acct1.slot = s.slot
        WHERE acct1.pubkey = input_pubkey
        AND s.slot <= max_slot
        AND s.status = commitment_level
        AND NOT EXISTS (
            SELECT NULL FROM account_audit AS acct2
            JOIN slot AS s2 ON acct2.slot = s2.slot
            WHERE s.slot < s2.slot
            AND acct2.pubkey = input_pubkey
            AND (commitment_level = 'processed' OR
                (commitment_level = 'confirmed' AND s2.status in ('confirmed', 'rooted')) OR
                (commitment_level = 'rooted' AND s2.status = 'rooted')
            )
            AND s2.slot <= max_slot
        );
    END IF;
    RETURN;
END;

$$;