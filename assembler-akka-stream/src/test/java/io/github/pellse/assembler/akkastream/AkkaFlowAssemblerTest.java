/*
 * Copyright 2018 Sebastien Pelletier
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.github.pellse.assembler.akkastream;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.testkit.javadsl.TestKit;
import io.github.pellse.assembler.*;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CompletionStage;

import static io.github.pellse.assembler.AssemblerBuilder.assemblerOf;
import static io.github.pellse.assembler.AssemblerTestUtils.*;
import static io.github.pellse.assembler.akkastream.AkkaSourceAdapter.akkaSourceAdapter;
import static io.github.pellse.util.query.MapperUtils.oneToManyAsList;
import static io.github.pellse.util.query.MapperUtils.oneToOne;
import static java.util.Arrays.asList;

public class AkkaFlowAssemblerTest {

    private final ActorSystem system = ActorSystem.create();
    private final Materializer mat = ActorMaterializer.create(system);

    private List<Customer> getCustomers() {
        return asList(customer1, customer2, customer3, customer1, customer2);
    }

    @Test
    public void testAssemblerBuilderWithAkkaFlowWithGroupingOutsideFlow() throws Exception {

        TestKit probe = new TestKit(system);

        Source<List<Customer>, NotUsed> customerSource = Source.from(getCustomers()).grouped(3);

        Flow<List<Customer>, Transaction, NotUsed> transactionFlow = Flow.<List<Customer>>create()
                .flatMapConcat(customerList ->
                        assemblerOf(Transaction.class)
                                .withIdExtractor(Customer::getCustomerId)
                                .withAssemblerRules(
                                        oneToOne(AssemblerTestUtils::getBillingInfos, BillingInfo::getCustomerId, BillingInfo::new),
                                        oneToManyAsList(AssemblerTestUtils::getAllOrders, OrderItem::getCustomerId),
                                        Transaction::new)
                                .using(akkaSourceAdapter(true))
                                .assemble(customerList)); // Parallel

        final CompletionStage<Done> future = customerSource.via(transactionFlow).runWith(
                Sink.foreach(elem -> probe.getRef().tell(elem, ActorRef.noSender())), mat);

        probe.expectMsgAllOf(transaction1, transaction2, transaction3, transaction1, transaction2);

        future.toCompletableFuture().get();
    }

    @Test
    public void testAssemblerBuilderWithAkkaFlowWithGroupingInsideFlow() throws Exception {

        TestKit probe = new TestKit(system);

        Source<Customer, NotUsed> customerSource = Source.from(getCustomers());

        Flow<Customer, Transaction, NotUsed> transactionFlow = Flow.<Customer>create()
                .grouped(3)
                .flatMapConcat(customerList ->
                        assemblerOf(Transaction.class)
                                .withIdExtractor(Customer::getCustomerId)
                                .withAssemblerRules(
                                        oneToOne(AssemblerTestUtils::getBillingInfos, BillingInfo::getCustomerId, BillingInfo::new),
                                        oneToManyAsList(AssemblerTestUtils::getAllOrders, OrderItem::getCustomerId),
                                        Transaction::new)
                                .using(akkaSourceAdapter(Source::async))
                                .assemble(customerList)); // Custom underlying sources configuration

        final CompletionStage<Done> future = customerSource.via(transactionFlow).runWith(
                Sink.foreach(elem -> probe.getRef().tell(elem, ActorRef.noSender())), mat);

        probe.expectMsgAllOf(transaction1, transaction2, transaction3, transaction1, transaction2);

        future.toCompletableFuture().get();
    }

    @Test
    public void testReusableAssemblerBuilderWithAkkaFlowWithGroupingInsideFlow() throws Exception {

        TestKit probe = new TestKit(system);

        Assembler<Customer, Source<Transaction, ?>> assembler = assemblerOf(Transaction.class)
                .withIdExtractor(Customer::getCustomerId)
                .withAssemblerRules(
                        oneToOne(AssemblerTestUtils::getBillingInfos, BillingInfo::getCustomerId, BillingInfo::new),
                        oneToManyAsList(AssemblerTestUtils::getAllOrders, OrderItem::getCustomerId),
                        Transaction::new)
                .using(akkaSourceAdapter(Source::async));

        Source<Customer, NotUsed> customerSource = Source.from(getCustomers());

        Flow<Customer, Transaction, NotUsed> transactionFlow = Flow.<Customer>create()
                .grouped(3)
                .flatMapConcat(assembler::assemble); // Custom underlying sources configuration

        final CompletionStage<Done> future = customerSource.via(transactionFlow).runWith(
                Sink.foreach(elem -> probe.getRef().tell(elem, ActorRef.noSender())), mat);

        probe.expectMsgAllOf(transaction1, transaction2, transaction3, transaction1, transaction2);

        future.toCompletableFuture().get();
    }
}
