import asyncio
import sys
import requests
from mcp.server.models import InitializationOptions
from mcp.server import NotificationOptions, Server
import mcp.types as types
import mcp.server.stdio

# URL pointing to your live running FastAPI app
BASE_API_URL = "http://localhost:8000/api/v1"

server = Server("acme-financial-dwh-mcp")

@server.list_tools()
async def handle_list_tools() -> list[types.Tool]:
    """Exposes your Data Warehouse endpoints as tools the LLM can use."""
    return [
        types.Tool(
            name="list_assets",
            description="Fetch a list of all active financial assets available in the data warehouse.",
            inputSchema={"type": "object", "properties": {}}
        ),
        types.Tool(
            name="fetch_asset_details",
            description="Get complete metadata profiles and dynamic attributes of a specific asset by its ID.",
            inputSchema={
                "type": "object",
                "properties": {"assetId": {"type": "string", "description": "The unique asset identifier (e.g., GM_STOCK)"}},
                "required": ["assetId"]
            }
        ),
        types.Tool(
            name="calculate_metrics",
            description="Get calculated analytics (min, max, average) for an asset's time-series data.",
            inputSchema={
                "type": "object",
                "properties": {
                    "assetId": {"type": "string", "description": "The asset ID"},
                    "metric_key": {"type": "string", "description": "The indicator key, e.g., 'close'"}
                },
                "required": ["assetId", "metric_key"]
            }
        )
    ]

@server.call_tool()
async def handle_call_tool(name: str, arguments: dict | None) -> list[types.TextContent]:
    """Executes the tool chosen by the LLM by pulling live data from your FastAPI app."""
    if not arguments:
        arguments = {}
        
    try:
        if name == "list_assets":
            res = requests.get(f"{BASE_API_URL}/assets")
            return [types.TextContent(type="text", text=str(res.json()))]
            
        elif name == "fetch_asset_details":
            asset_id = arguments.get("assetId")
            res = requests.get(f"{BASE_API_URL}/assets/{asset_id}")
            return [types.TextContent(type="text", text=str(res.json()))]
            
        elif name == "calculate_metrics":
            asset_id = arguments.get("assetId")
            metric_key = arguments.get("metric_key")
            res = requests.get(f"{BASE_API_URL}/analytics/aggregate", params={"assetId": asset_id, "metric_key": metric_key})
            return [types.TextContent(type="text", text=str(res.json()))]
            
        else:
            raise ValueError(f"Unknown tool requested: {name}")
            
    except Exception as err:
        return [types.TextContent(type="text", text=f"Error executing platform tool: {str(err)}")]

async def main():
    # Run the server using standard input/output channels (how LLMs talk to local scripts)
    async with mcp.server.stdio.stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            InitializationOptions(
                server_name="acme-financial-dwh-mcp",
                server_version="1.0.0",
                capabilities=server.get_capabilities(
                    notification_options=NotificationOptions(),
                    experimental_capabilities={},
                ),
            ),
        )

if __name__ == "__main__":
    asyncio.run(main())