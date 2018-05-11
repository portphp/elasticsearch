<?php declare(strict_types=1);

namespace Port\Elasticsearch;

use Elasticsearch\Client;
use Port\Reader;

class ScrollReader implements Reader
{
    private $client;

    private $query;

    private $scrollTTL = '1m';

    public function __construct($client, array $query)
    {
        $this->client = $client;
        $this->query = $query;
    }

    public function setScrollTTL(string $ttl): void
    {
        $this->scrollTTL = $ttl;
    }

    public function getItems(): \Generator
    {
        $query = $this->query;
        $query['scroll'] = $this->scollTTL;

        $response = $this->client->search($this->query);

        while (isset($response['hits']['hits']) && count($response['hits']['hits']) > 0) {

            foreach ($response['hits']['hits'] as $doc) {
                yield $doc;
            }
        
            $response = $client->scroll([
                    'scroll_id' => $response['_scroll_id'],
                    'scroll' => $this->scrollTTL,
                ]
            );
        }
    }
}