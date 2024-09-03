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

import java.util.List;

import static io.github.pellse.assembler.Assembler.assemble;
import static io.github.pellse.assembler.AssemblerBuilder.assemblerOf;
import static io.github.pellse.assembler.Rule.rule;
import static io.github.pellse.assembler.RuleMapper.oneToMany;
import static io.github.pellse.assembler.RuleMapper.oneToOne;
import static io.github.pellse.assembler.RuleMapperSource.call;

public class EmbeddedAssemblerTest {

    record Post(PostDetails postDetails, List<PostComment> comments, List<PostTag> postTags) {
    }

    record PostDetails(Long id, String title) {
    }

    record PostComment(Float id, Long postId, String review, List<UserVote> userVotes) {

        PostComment(PostComment postComment, List<UserVote> userVotes) {
            this(postComment.id, postComment.postId, postComment.review, userVotes);
        }
    }

    record UserVoteView(Integer id, Float commentId, Double userId, int score) {
    }

    record UserVote(Integer id, Float commentId, User user, int score) {

        UserVote(UserVoteView userVoteView, User user) {
            this(userVoteView.id, userVoteView.commentId, user, userVoteView.score);
        }
    }

    record User(Double id, String firstName, String lastName) {
    }

    record PostTag(Short id, Long postId, String name) {
    }

    Flux<PostDetails> getPostDetails() {
        return Flux.just();
    }

    Flux<PostComment> getPostCommentsById(List<Long> postIds) {
        return Flux.just();
    }

    Flux<UserVoteView> getUserVoteViewsById(List<Float> postCommentIds) {
        return Flux.just();
    }

    Flux<User> getUsersById(List<Double> userIds) {
        return Flux.just();
    }

    Flux<PostTag> getPostTag(List<Long> postIds) {
        return Flux.just();
    }

    @Test
    public void testWithGetRepliesById() {

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

        Assembler<PostDetails, Post> assembler = assemblerOf(Post.class)
                .withCorrelationIdResolver(PostDetails::id)
                .withRules(
                        rule(PostComment::postId, oneToMany(PostComment::id, call(assemble(this::getPostCommentsById, postCommentAssembler)))),
                        rule(PostTag::postId, oneToMany(PostTag::id, call(this::getPostTag))),
                        Post::new)
                .build();

        Flux<Post> posts = assembler.assemble(getPostDetails());
        posts.subscribe();
    }
}
