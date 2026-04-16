from fastapi import FastAPI
from strawberry.fastapi import GraphQLRouter
from api.schema import schema

app = FastAPI(title="NYC Transit API")
graphql_app = GraphQLRouter(schema)
app.include_router(graphql_app, prefix="/graphql")