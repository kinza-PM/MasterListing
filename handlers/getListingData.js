import { DynamoDBClient, ScanCommand, QueryCommand } from "@aws-sdk/client-dynamodb";
import { unmarshall } from "@aws-sdk/util-dynamodb";

const REGION = process.env.REGION;
const STAGE = process.env.STAGE || "dev";
const META_SYNC_LISTING_TABLE = process.env.META_SYNC_LISTING_TABLE;

const ddbClient = new DynamoDBClient({ region: REGION });

export const handler = async (event) => {
    try {
        // Parse query parameters
        let { tableName, nextToken, limit, search, browserId, sortBy, sortOrder = "asc" } = event.queryStringParameters || {};
        let items = []

        if (!tableName) {
            return {
                statusCode: 400,
                body: JSON.stringify({ message: "Missing required parameter: tableName" }),
            };
        }

        const targetTableName = `${tableName}-${STAGE}`;

        // Validate tableName exists in metaSync
        // const metaQuery = new QueryCommand({
        //     TableName: META_SYNC_LISTING_TABLE,
        //     KeyConditionExpression: "tableName = :t",
        //     ExpressionAttributeValues: {
        //         ":t": { S: targetTableName }
        //     },
        // });

        // const metaResp = await ddbClient.send(metaQuery);

        // const metaItem = metaResp.Items?.length ? metaResp.Items[0] : null;

        // if (!metaItem) {
        //     return {
        //         statusCode: 400,
        //         body: JSON.stringify({
        //             message: `Table '${targetTableName}' not found in meta-sync listing. Please verify tableName.`,
        //         }),
        //     };
        // }


        if (search && search.trim() !== "") {

            if (search.length === 3) {
                const queryParams = {
                    TableName: targetTableName,
                    IndexName: "GSI_IATA_CODE",
                    KeyConditionExpression: "iataCode = :iataCode",
                    ExpressionAttributeValues: {
                        ":iataCode": { S: search.toUpperCase() },
                    }
                    // Limit: 100,
                };
                const dataResp = await ddbClient.send(new QueryCommand(queryParams));
                
                items.push(...(dataResp.Items || []).map(item => unmarshall(item)));
            }

            let ExclusiveStartKey = undefined;
            do {
                const scanParams = {
                    TableName: targetTableName,
                    ExclusiveStartKey,
                    Limit: 500,
                    FilterExpression: "begins_with(lowerCountry, :search) OR begins_with(lowerCity, :search) OR begins_with(lowerAirportName, :search)",
                    ExpressionAttributeValues: {
                        ":search": { S: search.toLowerCase() },
                    },
                };

                const dataResp = await ddbClient.send(new ScanCommand(scanParams));
                items.push(...(dataResp.Items || []).map(item => unmarshall(item)));
                ExclusiveStartKey = dataResp.LastEvaluatedKey;
            } while (ExclusiveStartKey);

            if (browserId) {
                const params = {
                    TableName: process.env.USER_SEARCH_PREFERENCES_TABLE,
                    KeyConditionExpression: "userKey = :userKey",
                    ExpressionAttributeValues: {
                        ":userKey": { S: browserId }, // ✅ FIXED
                    },
                    ScanIndexForward: false, // DESC order
                    Limit: 3,
                };

                const result = await ddbClient.send(new QueryCommand(params));

                items.push(...(result.Items || []).map(item => unmarshall(item)));
            }



        } else {
            // Normal paginated scan
            const scanParams = {
                TableName: targetTableName,
                ExclusiveStartKey: nextToken
                    ? JSON.parse(Buffer.from(nextToken, "base64").toString("utf8"))
                    : undefined,
            };

            const dataResp = await ddbClient.send(new ScanCommand(scanParams));
            items = (dataResp.Items || []).map(item => unmarshall(item));

            // Alphabetical ordering if table is "countries-listing"
            if (targetTableName === "countries-listing-dev") {
                items.sort((a, b) => a.city.localeCompare(b.city));
            }

            if (sortBy) {
                items.sort((a, b) => {
                    const valA = a?.[sortBy];
                    const valB = b?.[sortBy];

                    // handle null/undefined
                    if (valA == null && valB == null) return 0;
                    if (valA == null) return 1;
                    if (valB == null) return -1;

                    // numeric sort (e.g. sequence)
                    if (typeof valA === "number" && typeof valB === "number") {
                        return sortOrder === "desc" ? valB - valA : valA - valB;
                    }

                    // string sort (e.g. city, name)
                    return sortOrder === "desc"
                        ? String(valB).localeCompare(String(valA))
                        : String(valA).localeCompare(String(valB));
                });
            }

            // Return nextToken for client
            nextToken = dataResp.LastEvaluatedKey
                ? Buffer.from(JSON.stringify(dataResp.LastEvaluatedKey)).toString("base64")
                : null;
        }


        return {
            statusCode: 200,
            headers: {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Credentials": true,
            },
            body: JSON.stringify({
                items,
                nextToken,
            }),
        };
    } catch (error) {
        console.error("Error fetching listing datass:", error);
        return {
            statusCode: 500,
            body: JSON.stringify({
                message: "Internal server error",
                error: error.message || error,
            }),
        };
    }
};

async function searchLocation(search, targetTable) {
    const searchLower = search.toLowerCase();

    try {
        // 1️⃣ Country exact match
        const countryResult = await ddbClient.send(
            new QueryCommand({
                TableName: targetTable,
                IndexName: "GSI_LowerCountry_LowerCity",
                KeyConditionExpression: "lowerCountry = :search",
                ExpressionAttributeValues: {
                    ":search": { S: searchLower }
                }
            })
        );

        // 2️⃣ City prefix match
        const cityResult = await ddbClient.send(
            new QueryCommand({
                TableName: targetTable,
                IndexName: "GSI_LowerCity_LowerCountry",
                KeyConditionExpression: "begins_with(lowerCity, :search)",
                ExpressionAttributeValues: {
                    ":search": { S: searchLower }
                }
            })
        );

        // 3️⃣ Merge + remove duplicates
        const combined = [...countryResult.Items, ...cityResult.Items];
        const unique = Array.from(
            new Map(combined.map(i => [i.country.S + "|" + i.city.S, i])).values()
        );

        return unique;

    } catch (err) {
        console.error("Error searching locations:", err);
        return [];
    }
}

