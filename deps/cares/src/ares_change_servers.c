
#include "ares_setup.h"
#include "ares.h"
#include "ares_private.h"

int ares_rebuild_queries_list(ares_channel channel, int skip_first, int skip_last, bool is_shift);
int ares_close_old_servers(ares_channel channel);
int ares_server_shift(ares_channel channel);
int ares_finish_change_servers(ares_channel channel);

/**
*   Add new servers to the channel
*/

int ares_change_servers(ares_channel channel, struct ares_addr_node* source_servers)
{
      
    struct ares_addr_node *source_server;
    int nservers;      

    struct server_state *server;
            
    int new_servers_count = 0;
    
    int i;
    
    for (source_server = source_servers; source_server; source_server = source_server->next)
    {
        new_servers_count++;
    }
        
    nservers = channel->nservers + new_servers_count;
            
    channel->servers = realloc(channel->servers, nservers * sizeof(struct server_state));
    
    if (!channel->servers) {   
        return ARES_ENOMEM;
    }
                         
    for (i = channel->nservers, source_server = source_servers; i < nservers; i++, source_server = source_server->next)  
    {
                        
        server = &channel->servers[i];                             
        
        server->addr.family = source_server->family;
                                                                                              
        if (source_server->family == AF_INET)
            memcpy(&channel->servers[i].addr.addrV4, &source_server->addrV4,
                   sizeof(source_server->addrV4));
        else
            memcpy(&channel->servers[i].addr.addrV6, &source_server->addrV6,
                   sizeof(source_server->addrV6));                   
                     
        /* 
        *   Watch ares__init_servers_state() 
        */
        
        server->udp_socket = ARES_SOCKET_BAD;
        server->tcp_socket = ARES_SOCKET_BAD;
        server->tcp_connection_generation = ++channel->tcp_connection_generation;
        server->tcp_lenbuf_pos = 0;
        server->tcp_buffer_pos = 0;
        server->tcp_buffer = NULL;
        server->tcp_length = 0;
        server->qhead = NULL;
        server->qtail = NULL;
        ares__init_list_head(&server->queries_to_server);
        server->channel = channel;  
        server->is_broken = 0;       
                        
    }    
    
    channel->nservers_change = channel->nservers; /* how many servers will be shifted */    
    
    /*
    *   First new server will be used for new dns requests
    */
    
    channel->last_server = channel->nservers;
    channel->nservers = nservers;         
    
    channel->rotation_state = channel->rotate; /* save rotation state */  
    channel->rotate = 0; /* turn off rotation, use one server */      
            
    /* 
    *   Need to change references in all queries to new server->queries_to_server reference. Skip last new servers, because they have right references in lists
    */
            
    ares_rebuild_queries_list(channel, 0, new_servers_count, false);    
        
    return ARES_SUCCESS;  
    
}

/*
*   Close old servers if they finish all queries.
*   Call at the end of ares process pool.
*/

int ares_close_old_servers(ares_channel channel)
{
      
    struct server_state *server;
    int i;
    int status;  
        
    if (channel->nservers_change < 1) 
    {        
        /* 
        *   Now nothing is changing.
        */        
        
        return ARES_SUCCESS;        
    }   
    
    if (channel->servers)
    {
        for (i = 0; i < channel->last_server; i++)
        {
                                
            server = &channel->servers[i];
            
            if (ares__is_list_empty(&server->queries_to_server))
            {
                             
                ares__close_sockets(channel, server); 
                               
                status = ares_server_shift(channel);
                
                if (status != ARES_SUCCESS)
                {
                    return status;
                }
                
                ares_rebuild_queries_list(channel, i, 0, true);
    
                if (channel->nservers_change > 0)
                {        
                    channel->nservers_change--;
                }
                
                i--;
            }
                          
        }     
        
        if (channel->last_server == 0)
        {
            ares_finish_change_servers(channel);    
        }   
                        
    }     
            
    return ARES_SUCCESS;
    
}

/* 
*   Remove first server from channel.
*/

int ares_server_shift(ares_channel channel)
{    
        
    int i;
    int nservers = channel->nservers - 1;

    struct server_state *servers_buff = malloc(nservers * sizeof(struct server_state));      
    
    if (!servers_buff) {   
        return ARES_ENOMEM;
    }
    
    for (i = 1; i < channel->nservers; i++)
    {        
        servers_buff[i - 1] = channel->servers[i];        
    }    
            
    free(channel->servers); 
        
    channel->servers = servers_buff;  
    channel->nservers = nservers;
    channel->last_server--;                
            
    return ARES_SUCCESS;
    
}

/*
*   Rebuild links in linked lists server->queries_to_server. Used after channel->servers memory relocation
*/

int ares_rebuild_queries_list(ares_channel channel, int skip_first, int skip_last, bool is_shift)
{
               
    struct server_state *server;
    
    struct list_node* list_head;
    struct list_node* list_node;
    
    struct query *query;
        
    int i;

    for (i = 0; i < (channel->nservers - skip_last); i++)
    {
                
        server = &channel->servers[i];
        
        list_head = &server->queries_to_server;
                        
        if (list_head->next->data == NULL) 
        {         
            /* 
            *   "empty list" with old "next" and "prev" pointer to head. "list_head->next" same "list_head"
            */
            
            ares__init_list_head(list_head);          
        }
        else
        {                                    
            /*
            *   Change next and previous link in neighbor nodes
            */           
            
            list_head->prev->next = list_head;
            list_head->next->prev = list_head;
                        
            /*
            * Servers can be shifted before query will be responsed. query save server id in query->server. 
            * Decrement query->server of each query of this server->queries_to_server.
            * Is used after shift one server.
            */
            
            if(is_shift && i >= skip_first)
            {
                for (list_node = list_head->next; list_node != list_head; list_node = list_node->next)
                {
                    query = list_node->data;    
                    query->server = query->server - 1;
                    
                }
            }                                   
        }                       
    }   
    
    return ARES_SUCCESS;
    
}

/* 
*   Restoration of the previous state of the channel.
*/

int ares_finish_change_servers(ares_channel channel)
{
     
    channel->rotate = channel->rotation_state;   
    channel->rotation_state = -1;
       
    return ARES_SUCCESS;       
    
}   