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

import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.github.pellse.assembler.Assembler.assemble;
import static io.github.pellse.assembler.AssemblerBuilder.assemblerOf;
import static io.github.pellse.assembler.Rule.rule;
import static io.github.pellse.assembler.RuleMapper.oneToMany;
import static io.github.pellse.assembler.RuleMapper.oneToOne;
import static io.github.pellse.assembler.RuleMapperSource.call;
import static reactor.core.scheduler.Schedulers.immediate;

record Post(PostDetails postDetails, List<PostComment> comments, List<PostTag> postTags) {
}

record PostDetails(Long id, String title) {
}

record PostComment(Long id, Long postId, String review, List<UserVote> userVotes) {

    PostComment(PostComment postComment, List<UserVote> userVotes) {
        this(postComment.id(), postComment.postId(), postComment.review(), userVotes);
    }
}

record UserVoteView(Long id, Long commentId, Long userId, int score) {
}

record UserVote(Long id, Long commentId, User user, int score) {

    UserVote(UserVoteView userVoteView, User user) {
        this(userVoteView.id(), userVoteView.commentId(), user, userVoteView.score());
    }
}

record User(Long id, String firstName, String lastName) {
}

record PostTag(Long id, Long postId, String name) {
}

public class EmbeddedAssemblerTest {

    PostDetails postDetails1 = new PostDetails(1L, "Title 1");
    PostDetails postDetails2 = new PostDetails(2L, "Title 2");
    PostDetails postDetails3 = new PostDetails(3L, "Title 3");
    PostDetails postDetails4 = new PostDetails(4L, "Title 4");
    PostDetails postDetails5 = new PostDetails(5L, "Title 5");

    // For postDetails1
    PostComment postComment1_1 = new PostComment(1L, 1L, "Review 1-1", null);
    PostComment postComment1_2 = new PostComment(2L, 1L, "Review 1-2", null);
    PostComment postComment1_3 = new PostComment(3L, 1L, "Review 1-3", null);

    // For postDetails2
    PostComment postComment2_1 = new PostComment(4L, 2L, "Review 2-1", null);
    PostComment postComment2_2 = new PostComment(5L, 2L, "Review 2-2", null);
    PostComment postComment2_3 = new PostComment(6L, 2L, "Review 2-3", null);

    // For postDetails3
    PostComment postComment3_1 = new PostComment(7L, 3L, "Review 3-1", null);
    PostComment postComment3_2 = new PostComment(8L, 3L, "Review 3-2", null);
    PostComment postComment3_3 = new PostComment(9L, 3L, "Review 3-3", null);

    // For postDetails4
    PostComment postComment4_1 = new PostComment(10L, 4L, "Review 4-1", null);
    PostComment postComment4_2 = new PostComment(11L, 4L, "Review 4-2", null);
    PostComment postComment4_3 = new PostComment(12L, 4L, "Review 4-3", null);

    // For postDetails5
    PostComment postComment5_1 = new PostComment(13L, 5L, "Review 5-1", null);
    PostComment postComment5_2 = new PostComment(14L, 5L, "Review 5-2", null);
    PostComment postComment5_3 = new PostComment(15L, 5L, "Review 5-3", null);

    // For postComment1_1
    UserVoteView userVoteView1_1_1 = new UserVoteView(1L, 1L, 1L, 5);
    UserVoteView userVoteView1_1_2 = new UserVoteView(2L, 1L, 2L, 3);
    UserVoteView userVoteView1_1_3 = new UserVoteView(3L, 1L, 3L, 4);

    // For postComment1_2
    UserVoteView userVoteView1_2_1 = new UserVoteView(4L, 2L, 4L, 2);
    UserVoteView userVoteView1_2_2 = new UserVoteView(5L, 2L, 5L, 4);
    UserVoteView userVoteView1_2_3 = new UserVoteView(6L, 2L, 6L, 3);

    // For postComment1_3
    UserVoteView userVoteView1_3_1 = new UserVoteView(7L, 3L, 1L, 1);
    UserVoteView userVoteView1_3_2 = new UserVoteView(8L, 3L, 2L, 2);
    UserVoteView userVoteView1_3_3 = new UserVoteView(9L, 3L, 3L, 5);

    // For postComment2_1
    UserVoteView userVoteView2_1_1 = new UserVoteView(10L, 4L, 4L, 5);
    UserVoteView userVoteView2_1_2 = new UserVoteView(11L, 4L, 5L, 3);
    UserVoteView userVoteView2_1_3 = new UserVoteView(12L, 4L, 6L, 4);

    // For postComment2_2
    UserVoteView userVoteView2_2_1 = new UserVoteView(13L, 5L, 1L, 2);
    UserVoteView userVoteView2_2_2 = new UserVoteView(14L, 5L, 2L, 1);
    UserVoteView userVoteView2_2_3 = new UserVoteView(15L, 5L, 3L, 3);

    // For postComment2_3
    UserVoteView userVoteView2_3_1 = new UserVoteView(16L, 6L, 4L, 4);
    UserVoteView userVoteView2_3_2 = new UserVoteView(17L, 6L, 5L, 5);
    UserVoteView userVoteView2_3_3 = new UserVoteView(18L, 6L, 6L, 2);

    // For postComment3_1
    UserVoteView userVoteView3_1_1 = new UserVoteView(19L, 7L, 1L, 3);
    UserVoteView userVoteView3_1_2 = new UserVoteView(20L, 7L, 2L, 4);
    UserVoteView userVoteView3_1_3 = new UserVoteView(21L, 7L, 3L, 5);

    // For postComment3_2
    UserVoteView userVoteView3_2_1 = new UserVoteView(22L, 8L, 4L, 2);
    UserVoteView userVoteView3_2_2 = new UserVoteView(23L, 8L, 5L, 1);
    UserVoteView userVoteView3_2_3 = new UserVoteView(24L, 8L, 6L, 4);

    // For postComment3_3
    UserVoteView userVoteView3_3_1 = new UserVoteView(25L, 9L, 1L, 3);
    UserVoteView userVoteView3_3_2 = new UserVoteView(26L, 9L, 2L, 4);
    UserVoteView userVoteView3_3_3 = new UserVoteView(27L, 9L, 3L, 5);

    // For postComment4_1
    UserVoteView userVoteView4_1_1 = new UserVoteView(28L, 10L, 4L, 2);
    UserVoteView userVoteView4_1_2 = new UserVoteView(29L, 10L, 5L, 1);
    UserVoteView userVoteView4_1_3 = new UserVoteView(30L, 10L, 6L, 4);

    // For postComment4_2
    UserVoteView userVoteView4_2_1 = new UserVoteView(31L, 11L, 1L, 5);
    UserVoteView userVoteView4_2_2 = new UserVoteView(32L, 11L, 2L, 3);
    UserVoteView userVoteView4_2_3 = new UserVoteView(33L, 11L, 3L, 4);

    // For postComment4_3
    UserVoteView userVoteView4_3_1 = new UserVoteView(34L, 12L, 4L, 2);
    UserVoteView userVoteView4_3_2 = new UserVoteView(35L, 12L, 5L, 1);
    UserVoteView userVoteView4_3_3 = new UserVoteView(36L, 12L, 6L, 3);

    // For postComment5_1
    UserVoteView userVoteView5_1_1 = new UserVoteView(37L, 13L, 1L, 4);
    UserVoteView userVoteView5_1_2 = new UserVoteView(38L, 13L, 2L, 5);
    UserVoteView userVoteView5_1_3 = new UserVoteView(39L, 13L, 3L, 2);

    // For postComment5_2
    UserVoteView userVoteView5_2_1 = new UserVoteView(40L, 14L, 4L, 3);
    UserVoteView userVoteView5_2_2 = new UserVoteView(41L, 14L, 5L, 4);
    UserVoteView userVoteView5_2_3 = new UserVoteView(42L, 14L, 6L, 5);

    // For postComment5_3
    UserVoteView userVoteView5_3_1 = new UserVoteView(43L, 15L, 1L, 2);
    UserVoteView userVoteView5_3_2 = new UserVoteView(44L, 15L, 2L, 1);
    UserVoteView userVoteView5_3_3 = new UserVoteView(45L, 15L, 3L, 4);

    // For userVoteView
    User user1 = new User(1L, "Alice", "Smith");
    User user2 = new User(2L, "Bob", "Jones");
    User user3 = new User(3L, "Charlie", "Brown");
    User user4 = new User(4L, "David", "Wilson");
    User user5 = new User(5L, "Eve", "Johnson");
    User user6 = new User(6L, "Frank", "Miller");

    // For postDetails1
    PostTag postTag1_1 = new PostTag(1L, 1L, "Java");
    PostTag postTag1_2 = new PostTag(2L, 1L, "Spring");

    // For postDetails2
    PostTag postTag2_1 = new PostTag(3L, 2L, "Reactive");
    PostTag postTag2_2 = new PostTag(4L, 2L, "WebFlux");

    // For postDetails3
    PostTag postTag3_1 = new PostTag(5L, 3L, "Microservices");
    PostTag postTag3_2 = new PostTag(6L, 3L, "Cloud");

    // For postDetails4
    PostTag postTag4_1 = new PostTag(7L, 4L, "Kotlin");
    PostTag postTag4_2 = new PostTag(8L, 4L, "Coroutines");

    // For postDetails5
    PostTag postTag5_1 = new PostTag(9L, 5L, "DevOps");
    PostTag postTag5_2 = new PostTag(10L, 5L, "CI/CD");

    // Expected Posts

    // Post 1
    Post post1 = new Post(
            postDetails1,
            List.of(
                    new PostComment(
                            postComment1_1,
                            List.of(
                                    new UserVote(userVoteView1_1_1, user1),
                                    new UserVote(userVoteView1_1_2, user2),
                                    new UserVote(userVoteView1_1_3, user3)
                            )
                    ),
                    new PostComment(
                            postComment1_2,
                            List.of(
                                    new UserVote(userVoteView1_2_1, user4),
                                    new UserVote(userVoteView1_2_2, user5),
                                    new UserVote(userVoteView1_2_3, user6)
                            )
                    ),
                    new PostComment(
                            postComment1_3,
                            List.of(
                                    new UserVote(userVoteView1_3_1, user1),
                                    new UserVote(userVoteView1_3_2, user2),
                                    new UserVote(userVoteView1_3_3, user3)
                            )
                    )
            ),
            List.of(postTag1_1, postTag1_2)
    );

    // Post 2
    Post post2 = new Post(
            postDetails2,
            List.of(
                    new PostComment(
                            postComment2_1,
                            List.of(
                                    new UserVote(userVoteView2_1_1, user4),
                                    new UserVote(userVoteView2_1_2, user5),
                                    new UserVote(userVoteView2_1_3, user6)
                            )
                    ),
                    new PostComment(
                            postComment2_2,
                            List.of(
                                    new UserVote(userVoteView2_2_1, user1),
                                    new UserVote(userVoteView2_2_2, user2),
                                    new UserVote(userVoteView2_2_3, user3)
                            )
                    ),
                    new PostComment(
                            postComment2_3,
                            List.of(
                                    new UserVote(userVoteView2_3_1, user4),
                                    new UserVote(userVoteView2_3_2, user5),
                                    new UserVote(userVoteView2_3_3, user6)
                            )
                    )
            ),
            List.of(postTag2_1, postTag2_2)
    );

    // Post 3
    Post post3 = new Post(
            postDetails3,
            List.of(
                    new PostComment(
                            postComment3_1,
                            List.of(
                                    new UserVote(userVoteView3_1_1, user1),
                                    new UserVote(userVoteView3_1_2, user2),
                                    new UserVote(userVoteView3_1_3, user3)
                            )
                    ),
                    new PostComment(
                            postComment3_2,
                            List.of(
                                    new UserVote(userVoteView3_2_1, user4),
                                    new UserVote(userVoteView3_2_2, user5),
                                    new UserVote(userVoteView3_2_3, user6)
                            )
                    ),
                    new PostComment(
                            postComment3_3,
                            List.of(
                                    new UserVote(userVoteView3_3_1, user1),
                                    new UserVote(userVoteView3_3_2, user2),
                                    new UserVote(userVoteView3_3_3, user3)
                            )
                    )
            ),
            List.of(postTag3_1, postTag3_2)
    );

    // Post 4
    Post post4 = new Post(
            postDetails4,
            List.of(
                    new PostComment(
                            postComment4_1,
                            List.of(
                                    new UserVote(userVoteView4_1_1, user4),
                                    new UserVote(userVoteView4_1_2, user5),
                                    new UserVote(userVoteView4_1_3, user6)
                            )
                    ),
                    new PostComment(
                            postComment4_2,
                            List.of(
                                    new UserVote(userVoteView4_2_1, user1),
                                    new UserVote(userVoteView4_2_2, user2),
                                    new UserVote(userVoteView4_2_3, user3)
                            )
                    ),
                    new PostComment(
                            postComment4_3,
                            List.of(
                                    new UserVote(userVoteView4_3_1, user4),
                                    new UserVote(userVoteView4_3_2, user5),
                                    new UserVote(userVoteView4_3_3, user6)
                            )
                    )
            ),
            List.of(postTag4_1, postTag4_2)
    );

    // Post 5
    Post post5 = new Post(
            postDetails5,
            List.of(
                    new PostComment(
                            postComment5_1,
                            List.of(
                                    new UserVote(userVoteView5_1_1, user1),
                                    new UserVote(userVoteView5_1_2, user2),
                                    new UserVote(userVoteView5_1_3, user3)
                            )
                    ),
                    new PostComment(
                            postComment5_2,
                            List.of(
                                    new UserVote(userVoteView5_2_1, user4),
                                    new UserVote(userVoteView5_2_2, user5),
                                    new UserVote(userVoteView5_2_3, user6)
                            )
                    ),
                    new PostComment(
                            postComment5_3,
                            List.of(
                                    new UserVote(userVoteView5_3_1, user1),
                                    new UserVote(userVoteView5_3_2, user2),
                                    new UserVote(userVoteView5_3_3, user3)
                            )
                    )
            ),
            List.of(postTag5_1, postTag5_2)
    );

    List<Post> expectedPosts = IntStream.rangeClosed(1, 100)
            .boxed()
            .flatMap(__ -> Stream.of(post1, post2, post3, post4, post5))
            .toList();

    // API calls

    Flux<PostDetails> getPostDetails() {
        return Flux.just(postDetails1, postDetails2, postDetails3, postDetails4, postDetails5)
                .repeat(99);
    }

    Flux<PostComment> getPostCommentsById(List<Long> postIds) {
        return Flux.just(
                postComment1_1, postComment1_2, postComment1_3,
                postComment2_1, postComment2_2, postComment2_3,
                postComment3_1, postComment3_2, postComment3_3,
                postComment4_1, postComment4_2, postComment4_3,
                postComment5_1, postComment5_2, postComment5_3
        ).filter(postComment -> postIds.contains(postComment.postId()));
    }

    Flux<UserVoteView> getUserVoteViewsById(List<Long> postCommentIds) {
        return Flux.just(
                userVoteView1_1_1, userVoteView1_1_2, userVoteView1_1_3,
                userVoteView1_2_1, userVoteView1_2_2, userVoteView1_2_3,
                userVoteView1_3_1, userVoteView1_3_2, userVoteView1_3_3,
                userVoteView2_1_1, userVoteView2_1_2, userVoteView2_1_3,
                userVoteView2_2_1, userVoteView2_2_2, userVoteView2_2_3,
                userVoteView2_3_1, userVoteView2_3_2, userVoteView2_3_3,
                userVoteView3_1_1, userVoteView3_1_2, userVoteView3_1_3,
                userVoteView3_2_1, userVoteView3_2_2, userVoteView3_2_3,
                userVoteView3_3_1, userVoteView3_3_2, userVoteView3_3_3,
                userVoteView4_1_1, userVoteView4_1_2, userVoteView4_1_3,
                userVoteView4_2_1, userVoteView4_2_2, userVoteView4_2_3,
                userVoteView4_3_1, userVoteView4_3_2, userVoteView4_3_3,
                userVoteView5_1_1, userVoteView5_1_2, userVoteView5_1_3,
                userVoteView5_2_1, userVoteView5_2_2, userVoteView5_2_3,
                userVoteView5_3_1, userVoteView5_3_2, userVoteView5_3_3
        ).filter(userVoteView -> postCommentIds.contains(userVoteView.commentId()));
    }

    Flux<User> getUsersById(List<Long> userIds) {
        return Flux.just(user1, user2, user3, user4, user5, user6)
                .filter(user -> userIds.contains(user.id()));
    }

    Flux<PostTag> getPostTag(List<Long> postIds) {
        return Flux.just(
                postTag1_1, postTag1_2,
                postTag2_1, postTag2_2,
                postTag3_1, postTag3_2,
                postTag4_1, postTag4_2,
                postTag5_1, postTag5_2
        ).filter(postTag -> postIds.contains(postTag.postId()));
    }

    @Test
    public void testUserVoteAssembler() {

        Assembler<UserVoteView, UserVote> userVoteAssembler = assemblerOf(UserVote.class)
                .withCorrelationIdResolver(UserVoteView::id)
                .withRules(
                        rule(User::id, UserVoteView::userId, oneToOne(call(this::getUsersById))),
                        UserVote::new)
                .build(immediate());

        StepVerifier.create(userVoteAssembler.assemble(getUserVoteViewsById(List.of(13L, 14L, 15L))))
                .expectSubscription()
                .expectNextSequence(List.of(
                        new UserVote(userVoteView5_1_1, user1),
                        new UserVote(userVoteView5_1_2, user2),
                        new UserVote(userVoteView5_1_3, user3),
                        new UserVote(userVoteView5_2_1, user4),
                        new UserVote(userVoteView5_2_2, user5),
                        new UserVote(userVoteView5_2_3, user6),
                        new UserVote(userVoteView5_3_1, user1),
                        new UserVote(userVoteView5_3_2, user2),
                        new UserVote(userVoteView5_3_3, user3)))
                .expectComplete()
                .verify();
    }

    @Test
    public void testEmbeddedAssemblers() {

        Assembler<UserVoteView, UserVote> userVoteAssembler = assemblerOf(UserVote.class)
                .withCorrelationIdResolver(UserVoteView::id)
                .withRules(
                        rule(User::id, UserVoteView::userId, oneToOne(call(this::getUsersById))),
                        UserVote::new)
                .build();

        Assembler<PostComment, PostComment> postCommentAssembler = assemblerOf(PostComment.class)
                .withCorrelationIdResolver(PostComment::id)
                .withRules(
                        rule(UserVote::commentId, oneToMany(UserVote::id, call(assemble(this::getUserVoteViewsById, userVoteAssembler)))),
                        PostComment::new)
                .build();

        Assembler<PostDetails, Post> postAssembler = assemblerOf(Post.class)
                .withCorrelationIdResolver(PostDetails::id)
                .withRules(
                        rule(PostComment::postId, oneToMany(PostComment::id, call(assemble(this::getPostCommentsById, postCommentAssembler)))),
                        rule(PostTag::postId, oneToMany(PostTag::id, call(this::getPostTag))),
                        Post::new)
                .build();

        StepVerifier.create(getPostDetails()
                        .window(3)
                        .flatMapSequential(postAssembler::assemble))
                .expectSubscription()
                .expectNextSequence(expectedPosts)
                .verifyComplete();
    }
}
