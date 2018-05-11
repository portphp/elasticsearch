<?php

namespace Port\Elasticsearch\Tests;

use Port\Elasticsearch\BulkWriter;
use Elasticsearch\Client;
use PHPUnit\Framework\TestCase;

class BulkWriterTest extends TestCase
{
    protected function setup()
    {
        $this->client = $this->prophesize(Client::class);
        $this->writer = new BulkWriter($this->client->reveal(), ['_index' => 'foo']);

        $this->reflection = new \ReflectionProperty(BulkWriter::class, 'items');
        $this->reflection->setAccessible(true);
    }

    public function testPrepare()
    {
        $item = ['foo' => 'bar'];

        $this->writer->writeItem($item);

        $this->assertSame([$item], $this->reflection->getValue($this->writer));
    }

    public function testFinish()
    {
        $this->reflection->setValue($this->writer, [
            ['foo' => 'bar']
        ]);

        $this->client->bulk([
            '_index' => 'foo',
            'body' => [[
                'foo' => 'bar'
            ]]
        ])->shouldBeCalled();

        $this->writer->finish();

        $this->assertCount(0, $this->reflection->getValue($this->writer));
    }
}