import { DynamoDBClient, QueryCommand, PutItemCommand } from "@aws-sdk/client-dynamodb";
import { v4 as uuidv4 } from "uuid";

const REGION = process.env.REGION;
const STAGE = process.env.STAGE || "dev";
const META_SYNC_LISTING_TABLE = process.env.META_SYNC_LISTING_TABLE;

const ddbClient = new DynamoDBClient({ region: REGION });

export const handler = async (event) => {
    try {
        const body = typeof event.body === "string" ? JSON.parse(event.body) : event.body || {};

        // Step 1: Validate required field: tableName
        if (!body.tableName || typeof body.tableName !== "string") {
            return {
                statusCode: 400,
                body: JSON.stringify({ message: "Missing or invalid field: tableName" }),
            };
        }

        const targetTableName = `${body.tableName}-${STAGE}`;

        // Step 2: Fetch metadata from meta-sync table
        const metaResp = await ddbClient.send(
            new QueryCommand({
                TableName: META_SYNC_LISTING_TABLE,
                KeyConditionExpression: "tableName = :t",
                ExpressionAttributeValues: {
                    ":t": { S: targetTableName },
                },
            })
        );

        console.log("MetaSync Query Response:", metaResp);
        const metaItem = metaResp.Items?.length ? metaResp.Items[0] : null;

        if (!metaItem) {
            return {
                statusCode: 400,
                body: JSON.stringify({
                    message: `Table '${targetTableName}' not found in meta-sync listing. Please verify the table name.`,
                }),
            };
        }

        // Step 3: Validate required fields defined in meta
        const requiredFields = JSON.parse(metaItem.requiredFields?.S || "[]");

        for (const field of requiredFields) {
            const fieldName = field.column;
            const expectedType = field.type.toLowerCase();

            // Missing field
            if (!(fieldName in body)) {
                return {
                    statusCode: 400,
                    body: JSON.stringify({
                        message: `Missing required field: '${fieldName}'`,
                    }),
                };
            }

            // Type mismatch
            if (!validateType(body[fieldName], expectedType)) {
                return {
                    statusCode: 400,
                    body: JSON.stringify({
                        message: `Field '${fieldName}' must be of type '${expectedType}'`,
                    }),
                };
            }
        }

        // Step 4: Prepare item for insertion
        const nowEpoch = Date.now();
        const insertItem = {
            id: { S: body.id || uuidv4() },
            status: { N: "1" },
            createdAt: { N: nowEpoch.toString() },
            updatedAt: { N: nowEpoch.toString() },
        };

        // Include all other fields except excluded ones
        for (const [key, value] of Object.entries(body)) {
            if (key === "tableName" || key === "metaCreatedAt") continue;
            insertItem[key] = formatDynamoValue(value);
        }

        // Step 5: Insert into target table
        await ddbClient.send(
            new PutItemCommand({
                TableName: targetTableName,
                Item: insertItem,
            })
        );

        // Step 6: Return success response
        return {
            statusCode: 200,
            headers: {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Credentials": true,
            },
            body: JSON.stringify({ message: "Record inserted successfully" }),
        };
    } catch (error) {
        console.error("Error inserting table data:", error);
        return {
            statusCode: 500,
            body: JSON.stringify({
                message: "Internal server error",
                error: error.message || error,
            }),
        };
    }
};

// Utility: Type validation
function validateType(value, expectedType) {
    if (expectedType === "string") return typeof value === "string";
    if (expectedType === "number") return typeof value === "number";
    if (expectedType === "date") return !isNaN(Date.parse(value));
    if (expectedType === "boolean") return typeof value === "boolean";
    return true;
}

// Utility: Format values for DynamoDB
function formatDynamoValue(value) {
    if (typeof value === "string") return { S: value };
    if (typeof value === "number") return { N: value.toString() };
    if (typeof value === "boolean") return { BOOL: value };
    if (Array.isArray(value)) return { L: value.map((v) => formatDynamoValue(v)) };
    if (typeof value === "object" && value !== null)
        return { M: Object.fromEntries(Object.entries(value).map(([k, v]) => [k, formatDynamoValue(v)])) };

    return { S: JSON.stringify(value) }; // fallback
}
