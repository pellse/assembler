/*
 * Copyright 2017 Sebastien Pelletier
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

package io.github.pellse.assembler.flux;

import io.github.pellse.assembler.AssemblerTestUtils;
import io.github.pellse.assembler.AssemblerTestUtils.BillingInfo;
import io.github.pellse.assembler.AssemblerTestUtils.Customer;
import io.github.pellse.assembler.AssemblerTestUtils.OrderItem;
import io.github.pellse.assembler.AssemblerTestUtils.Transaction;
import org.junit.Test;
import reactor.core.publisher.Flux;

import java.util.List;

import static io.github.pellse.assembler.AssemblerTestUtils.customer1;
import static io.github.pellse.assembler.AssemblerTestUtils.customer2;
import static io.github.pellse.assembler.AssemblerTestUtils.customer3;
import static io.github.pellse.util.query.MapperUtils.oneToManyAsList;
import static io.github.pellse.util.query.MapperUtils.oneToOne;
import static java.time.Duration.ofMillis;
import static java.util.Arrays.asList;
import static reactor.core.publisher.Flux.interval;
import static reactor.core.publisher.Flux.zip;

public class FluxAssemblerTest {

    @Test
    public void testAssembleWithFlux() throws Exception {

        List<Customer> customerList = asList(customer1, customer2, customer3);

        Flux<Customer> customerFlux = Flux.generate(() -> 0, (index, sink) -> {
            sink.next(customerList.get(index));
            return ++index % 3;
        });
        Flux<Customer> intervalCustomerFlux = zip(customerFlux, interval(ofMillis(200)), (cust, l) -> cust);

        intervalCustomerFlux
                .log()
                .bufferTimeout(3, ofMillis(200))
                .flatMap(customers -> FluxAssembler.of(customers, Customer::getCustomerId)
                        .assemble(
                                oneToOne(AssemblerTestUtils::queryBillingInfoForAllCustomersIn, BillingInfo::getCustomerId),
                                oneToManyAsList(AssemblerTestUtils::queryAllOrdersForAllCustomersIn, OrderItem::getCustomerId),
                                Transaction::new))
                .doOnError(e -> System.out.println("error: " + e))
                .subscribe(e -> System.out.println(Thread.currentThread().getName() + ", " + e));

        Thread.sleep(500000);
    }
}
