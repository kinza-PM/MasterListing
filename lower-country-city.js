import { DynamoDBClient, ScanCommand, UpdateItemCommand } from "@aws-sdk/client-dynamodb";

// Initialize DynamoDB client
const ddbClient = new DynamoDBClient({ region: "eu-west-1" });

// Function to update lowercase fields with progress
async function updateLowercaseFields() {
    let ExclusiveStartKey = undefined;
    let totalUpdated = 0;

    do {
        // Scan table in batches
        const scanParams = {
            TableName: "countries-listing-dev",
            ExclusiveStartKey,
            ProjectionExpression: "country, city" // only fetch needed fields
        };

        const scanResult = await ddbClient.send(new ScanCommand(scanParams));
        ExclusiveStartKey = scanResult.LastEvaluatedKey;

        for (const item of scanResult.Items) {
            const country = item.country.S;
            const city = item.city.S;

            // Update the item with lowercase fields
            const updateParams = {
                TableName: "countries-listing-dev",
                Key: {
                    country: { S: country },
                    city: { S: city }
                },
                UpdateExpression: "SET lowerCountry = :lc, lowerCity = :lci",
                ExpressionAttributeValues: {
                    ":lc": { S: country.toLowerCase() },
                    ":lci": { S: city.toLowerCase() }
                }
            };

            try {
                await ddbClient.send(new UpdateItemCommand(updateParams));
                totalUpdated++;
                // Log every 10 rows or last row in batch
                if (totalUpdated % 10 === 0 || totalUpdated === scanResult.Items.length) {
                    console.log(`Updated ${totalUpdated} rows so far...`);
                }
            } catch (err) {
                console.error(`Failed to updatess: ${country} - ${city}`, err);
            }
        }

    } while (ExclusiveStartKey);

    console.log(`✅ All rows updated with lowercase fields! Total rows updated: ${totalUpdated}`);
}

// Run the script
updateLowercaseFields().catch(console.error);
