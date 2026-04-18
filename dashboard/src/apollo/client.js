import { ApolloClient, InMemoryCache, HttpLink } from '@apollo/client';

const client = new ApolloClient({
    link: new HttpLink({
        uri: import.meta.env.VITE_API_URL || 'http://localhost:8000/graphql',
    }),
    cache: new InMemoryCache(),
});

export default client;