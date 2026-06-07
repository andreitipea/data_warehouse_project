import asyncio
import sys
import requests
from mcp.server.models import InitializationOptions
from mcp.server import NotificationOptions, Server
import mcp.types as types
import mcp.server.stdio

BASE_API_URL = "http://localhost:8000/api/v1"
server = Server("acme-financial-dwh-mcp")

@server.list_tools()
async def handle_list_tools() -> list[types.Tool]:
    return [
        types.Tool(
            name="list_assets",
            description="Fetch a paginated list summary of active warehouse financial assets.",
            inputSchema={"type": "object", "properties": {}}
        ),
        types.Tool(
            name="calculate_spark_metrics",
            description="Execute core Apache Spark cluster statistics (min/max/average) over market feeds.",
            inputSchema={
                "type": "object",
                "properties": {
                    "assetId": {"type": "string"},
                    "dataSourceId": {"type": "string"}
                },
                "required": ["assetId", "dataSourceId"]
            }
        ),
        types.Tool(
            name="forecast_market_prices",
            description="Train a local Apache Spark ML Linear Regression model context to predict future values.",
            inputSchema={
                "type": "object",
                "properties": {
                    "assetId": {"type": "string"},
                    "dataSourceId": {"type": "string"}
                },
                "required": ["assetId", "dataSourceId"]
            }
        )
    ]

@server.call_tool()
async def handle_call_tool(name: str, arguments: dict | None) -> list[types.TextContent]:
    if not arguments: arguments = {}
    try:
        if name == "list_assets":
            res = requests.get(f"{BASE_API_URL}/assets")
            return [types.TextContent(type="text", text=str(res.json()))]
        elif name == "calculate_spark_metrics":
            res = requests.get(f"{BASE_API_URL}/analytics/aggregate", params=arguments)
            return [types.TextContent(type="text", text=str(res.json()))]
        elif name == "forecast_market_prices":
            res = requests.get(f"{BASE_API_URL}/analytics/predict", params=arguments)
            return [types.TextContent(type="text", text=str(res.json()))]
        else:
            raise ValueError(f"Unknown system execution variant: {name}")
    except Exception as err:
        return [types.TextContent(type="text", text=f"Tool error: {str(err)}")]

async def main():
    async with mcp.server.stdio.stdio_server() as (read_stream, write_stream):
        await server.run(read_stream, write_stream, InitializationOptions(
            server_name="acme-financial-dwh-mcp", server_version="2.0.0",
            capabilities=server.get_capabilities(notification_options=NotificationOptions(), experimental_capabilities={})
        ))

if __name__ == "__main__":
    asyncio.run(main())