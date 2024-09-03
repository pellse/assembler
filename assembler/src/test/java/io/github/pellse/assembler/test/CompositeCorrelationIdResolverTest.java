/*
 * Copyright 2024 Sebastien Pelletier
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

package io.github.pellse.assembler.test;

import io.github.pellse.assembler.Assembler;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.util.*;
import java.util.stream.Collectors;

import static io.github.pellse.assembler.AssemblerBuilder.assemblerOf;
import static io.github.pellse.assembler.Rule.rule;
import static io.github.pellse.assembler.RuleMapper.oneToMany;
import static io.github.pellse.assembler.RuleMapper.oneToOne;
import static io.github.pellse.assembler.RuleMapperSource.call;

public class CompositeCorrelationIdResolverTest {

    record Post(PostDetails post, User author, List<Reply> replies) {
    }

    record PostDetails(Long id, String userId, String content) {
    }

    record User(String Id, String username) {
    }

    record Reply(Long id, Long postId, String userId, String content) {
    }

    // Creating PostDetails records
    PostDetails postDetails1 = new PostDetails(1L, "user1", "Content of post 1");
    PostDetails postDetails2 = new PostDetails(2L, "user2", "Content of post 2");
    PostDetails postDetails3 = new PostDetails(3L, "user3", "Content of post 3");
    PostDetails postDetails4 = new PostDetails(4L, "user4", "Content of post 4");
    PostDetails postDetails5 = new PostDetails(5L, "user5", "Content of post 5");
    PostDetails postDetails6 = new PostDetails(6L, "user6", "Content of post 6");
    PostDetails postDetails7 = new PostDetails(7L, "user7", "Content of post 7");
    PostDetails postDetails8 = new PostDetails(8L, "user8", "Content of post 8");
    PostDetails postDetails9 = new PostDetails(9L, "user9", "Content of post 9");
    PostDetails postDetails10 = new PostDetails(10L, "user10", "Content of post 10");

    // Creating User records
    User user1 = new User("user1", "Alice");
    User user2 = new User("user2", "Bob");
    User user3 = new User("user3", "Charlie");
    User user4 = new User("user4", "David");
    User user5 = new User("user5", "Eve");
    User user6 = new User("user6", "Frank");
    User user7 = new User("user7", "Grace");
    User user8 = new User("user8", "Henry");
    User user9 = new User("user9", "Ivy");
    User user10 = new User("user10", "Jack");

    // Creating Reply records
    Reply reply1_1 = new Reply(1L, 1L, "user2", "Reply 1 to post 1");
    Reply reply1_2 = new Reply(2L, 1L, "user3", "Reply 2 to post 1");
    Reply reply1_3 = new Reply(3L, 1L, "user4", "Reply 3 to post 1");

    Reply reply2_1 = new Reply(4L, 2L, "user3", "Reply 1 to post 2");
    Reply reply2_2 = new Reply(5L, 2L, "user4", "Reply 2 to post 2");
    Reply reply2_3 = new Reply(6L, 2L, "user5", "Reply 3 to post 2");

    Reply reply3_1 = new Reply(7L, 3L, "user4", "Reply 1 to post 3");
    Reply reply3_2 = new Reply(8L, 3L, "user5", "Reply 2 to post 3");
    Reply reply3_3 = new Reply(9L, 3L, "user1", "Reply 3 to post 3");

    Reply reply4_1 = new Reply(10L, 4L, "user5", "Reply 1 to post 4");
    Reply reply4_2 = new Reply(11L, 4L, "user1", "Reply 2 to post 4");
    Reply reply4_3 = new Reply(12L, 4L, "user2", "Reply 3 to post 4");

    Reply reply5_1 = new Reply(13L, 5L, "user1", "Reply 1 to post 5");
    Reply reply5_2 = new Reply(14L, 5L, "user2", "Reply 2 to post 5");
    Reply reply5_3 = new Reply(15L, 5L, "user3", "Reply 3 to post 5");

    // Creating additional Reply records
    Reply reply6_1 = new Reply(16L, 6L, "user2", "Reply 1 to post 6");
    Reply reply7_1 = new Reply(17L, 7L, "user3", "Reply 1 to post 7");
    Reply reply8_1 = new Reply(18L, 8L, "user4", "Reply 1 to post 8");
    Reply reply9_1 = new Reply(19L, 9L, "user5", "Reply 1 to post 9");
    Reply reply10_1 = new Reply(20L, 10L, "user6", "Reply 1 to post 10");

    // Creating Post records with corresponding PostDetails, User, and Replies
    Post post1 = new Post(postDetails1, user1, List.of(reply1_1, reply1_2, reply1_3));
    Post post2 = new Post(postDetails2, user2, List.of(reply2_1, reply2_2, reply2_3));
    Post post3 = new Post(postDetails3, user3, List.of(reply3_1, reply3_2, reply3_3));
    Post post4 = new Post(postDetails4, user4, List.of(reply4_1, reply4_2, reply4_3));
    Post post5 = new Post(postDetails5, user5, List.of(reply5_1, reply5_2, reply5_3));
    Post post6 = new Post(postDetails6, user6, List.of(reply6_1));
    Post post7 = new Post(postDetails7, user7, List.of(reply7_1));
    Post post8 = new Post(postDetails8, user8, List.of(reply8_1));
    Post post9 = new Post(postDetails9, user9, List.of());
    Post post10 = new Post(postDetails10, user10, List.of());

    List<Post> expectedPosts = List.of(post1, post2, post3, post4, post5, post6, post7, post8, post9, post10);

    private Flux<PostDetails> getPostDetails() {
        return Flux.just(postDetails1, postDetails2, postDetails3, postDetails4, postDetails5, postDetails6, postDetails7, postDetails8, postDetails9, postDetails10);
    }

    private Flux<User> getUsersById(List<String> userIds) {
        return Flux.just(user1, user2, user3, user4, user5, user6, user7, user8, user9, user10)
                .filter(u -> userIds.contains(u.Id()));
    }

    private Flux<Reply> getReplies(List<PostDetails> postDetails) {

        return getRepliesByPostId(postDetails.stream()
                .map(PostDetails::id)
                .collect(Collectors.toSet()));
    }

    private Flux<Reply> getRepliesByPostId(Collection<Long> postDetailsIds) {
        return Flux.just(
                        reply1_1, reply1_2, reply1_3,
                        reply2_1, reply2_2, reply2_3,
                        reply3_1, reply3_2, reply3_3,
                        reply4_1, reply4_2, reply4_3,
                        reply5_1, reply5_2, reply5_3,
                        reply6_1,
                        reply7_1,
                        reply8_1
                )
                .filter(r -> postDetailsIds.contains(r.postId()));
    }

    /**
     * <p>The equivalent SQL query to retrieve the posts, authors, and replies if all data was in a single relation database:</p>
     *
     * <pre>{@code
     * SELECT
     *     p.id AS post_id,
     *     p.userId AS post_userId,
     *     p.content AS post_content,
     *     u.id AS author_id,
     *     u.username AS author_username,
     *     r.id AS reply_id,
     *     r.postId AS reply_postId,
     *     r.userId AS reply_userId,
     *     r.content AS reply_content
     * FROM
     *     PostDetails p
     * JOIN
     *     User u ON p.userId = u.id
     * LEFT JOIN
     *     Reply r ON p.id = r.postId
     * WHERE
     *     p.id IN (1, 2, 3);
     * }</pre>
     */
    @Test
    public void testWithGetReplies() {

        Assembler<PostDetails, Post> assembler = assemblerOf(Post.class)
                .withCorrelationIdResolver(PostDetails::id)
                .withRules(
                        rule(User::Id, PostDetails::userId, oneToOne(call(this::getUsersById))),
                        rule(Reply::postId, oneToMany(Reply::id, this::getReplies)),
                        Post::new)
                .build();

        StepVerifier.create(getPostDetails()
                        .window(3)
                        .flatMapSequential(assembler::assemble))
                .expectSubscription()
                .expectNextSequence(expectedPosts)
                .expectComplete()
                .verify();
    }

    @Test
    public void testWithGetRepliesByPostId() {

        Assembler<PostDetails, Post> assembler = assemblerOf(Post.class)
                .withCorrelationIdResolver(PostDetails::id)
                .withRules(
                        rule(User::Id, PostDetails::userId, oneToOne(call(this::getUsersById))),
                        rule(Reply::postId, oneToMany(Reply::id, call(this::getRepliesByPostId))),
                        Post::new)
                .build();

        StepVerifier.create(getPostDetails()
                        .window(3)
                        .flatMapSequential(assembler::assemble))
                .expectSubscription()
                .expectNextSequence(expectedPosts)
                .expectComplete()
                .verify();
    }
}
