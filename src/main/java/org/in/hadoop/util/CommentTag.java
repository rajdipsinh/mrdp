package org.in.hadoop.util;

/**
 * Date: 7/15/13
 * Time: 12:28 PM
 */
public enum CommentTag {

    ID("Id"),
    POSTID("PostId"),
    SCORE("Score"),
    TEXT("Text"),
    COMMENT("Comment"),
    CREATION_DATE("CreationDate"),
    USERID("UserId");

    private final String variable;


    CommentTag(String variable) {
        this.variable = variable;

    }

    @Override
    public String toString() {
       return variable;
    }

}
