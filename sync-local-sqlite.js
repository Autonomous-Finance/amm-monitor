import { createDataItemSigner, dryrun, connect } from "@permaweb/aoconnect";
import sqlite3 from 'sqlite3';
import { open } from 'sqlite';

async function dryRunAndUpsertToSQLite(processId, tableName) {
    try {
        if (!processId) {
            throw new Error('Process ID is not set.');
        }
        if (!tableName) {
            throw new Error('Table name is not set.');
        }

        const { dryrun } = connect();

        const wallet = JSON.parse(process.env.WALLET_JSON);

        const result = await dryrun({
            process: processId,
            data: '',
            tags: [
                { name: 'Action', value: 'Dump-Table-To-CSV' },
                { name: 'TableName', value: tableName }
            ],
            anchor: '1234',
            signer: createDataItemSigner(wallet),
        });

        console.log('Dry run result:', result);

        // Assuming the CSV data is in the first message's data field
        const csvData = result.Messages[0].Data;
        if (csvData === '') {
            console.log(`No data found for table ${tableName}`);
            return;
        }

        // Split CSV into lines
        const lines = csvData.trim().split('\n');
        const headers = lines[0].split(',');
        const rows = lines.slice(1);

        // Open SQLite database
        const db = await open({
            filename: 'local.sqlite',
            driver: sqlite3.Database
        });

        // Get table info to determine the primary key
        const tableInfo = await db.all(`PRAGMA table_info(${tableName})`);
        const primaryKey = tableInfo.find(column => column.pk === 1)?.name;

        if (!primaryKey) {
            throw new Error(`No primary key found for table ${tableName}`);
        }

        // Prepare upsert statement
        const placeholders = headers.map(() => '?').join(', ');
        const updateClauses = headers.filter(h => h !== primaryKey).map(header => `${header} = excluded.${header}`).join(', ');
        const upsertSQL = `
            INSERT INTO ${tableName} (${headers.join(', ')})
            VALUES (${placeholders})
            ON CONFLICT DO NOTHING
            `;

        /*
          ON CONFLICT(${primaryKey}) DO UPDATE SET
            ${updateClauses}*/

        const stmt = await db.prepare(upsertSQL);

        // Insert or update data
        for (const row of rows) {
            const values = row.split(',');
            await stmt.run(values);
        }

        await stmt.finalize();
        console.log(`Data upserted successfully into table ${tableName}`);
        await db.close();
    } catch (error) {
        console.error(`Error upserting data into table ${tableName}:`, error);
    }
}

dryRunAndUpsertToSQLite('snR3flTItDdCtCqHrJBWTV2vg0kj0onq2Wydlu-crhc', 'amm_registry')
dryRunAndUpsertToSQLite('snR3flTItDdCtCqHrJBWTV2vg0kj0onq2Wydlu-crhc', 'amm_swap_params')
dryRunAndUpsertToSQLite('snR3flTItDdCtCqHrJBWTV2vg0kj0onq2Wydlu-crhc', 'amm_swap_params_changes')
dryRunAndUpsertToSQLite('snR3flTItDdCtCqHrJBWTV2vg0kj0onq2Wydlu-crhc', 'amm_transactions')
dryRunAndUpsertToSQLite('snR3flTItDdCtCqHrJBWTV2vg0kj0onq2Wydlu-crhc', 'balances')
dryRunAndUpsertToSQLite('snR3flTItDdCtCqHrJBWTV2vg0kj0onq2Wydlu-crhc', 'indicator_subscriptions')
dryRunAndUpsertToSQLite('snR3flTItDdCtCqHrJBWTV2vg0kj0onq2Wydlu-crhc', 'token_registry')
dryRunAndUpsertToSQLite('snR3flTItDdCtCqHrJBWTV2vg0kj0onq2Wydlu-crhc', 'token_supply_changes')
dryRunAndUpsertToSQLite('snR3flTItDdCtCqHrJBWTV2vg0kj0onq2Wydlu-crhc', 'top_n_subscriptions')