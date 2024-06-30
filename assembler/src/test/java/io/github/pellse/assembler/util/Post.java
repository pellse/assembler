package io.github.pellse.assembler.util;

import java.util.List;

public record Post(PostDetails post, User author, List<Reply> replies) {
}
