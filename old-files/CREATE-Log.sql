SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[Log](
	[Id] [int] IDENTITY(1,1) NOT NULL,
	[datetime] [datetime] NULL,
	[scriptname] [nvarchar](250) NULL,
	[FunctionName] [nvarchar](250) NULL,
	[Message] [nvarchar](max) NULL,
	[Level] [nvarchar](30) NULL,
	[ExecutionId] [nvarchar](150) NULL,
	[ErrorDetails] [nvarchar](max) NULL,
	[ErrorLineNumber] [int] NULL,
	[ErrorLine] [nvarchar](max) NULL,
	[ScriptLineNumber] [int] NULL,
	[LoginName] [nvarchar](128) NULL,
	[HostName] [nvarchar](300) NULL,
	[Note] [nvarchar](max) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO

ALTER TABLE [dbo].[Log] ADD  DEFAULT (getdate()) FOR [datetime]
GO

CREATE PROCEDURE [dbo].[sp_log] @Message nvarchar(max), @ScriptName nvarchar(250), @FunctionName nvarchar(250), @ExecutionID nvarchar(150) , @Level nvarchar(30) = 'Info', @ErrorDetails nvarchar(max) = null, @ErrorLineNumber int = null, @ErrorLine nvarchar(max) = null, @ScriptLineNumber int = null, @Note nvarchar(MAX) = null
AS


INSERT INTO Log (Message, Scriptname, FunctionName, ExecutionID, Level, ErrorDetails, ErrorLineNumber, ErrorLine, ScriptLineNumber, LoginName, HostName, Note) VALUES (@Message, @ScriptName, @FunctionName, @ExecutionID, @Level, @ErrorDetails, @ErrorLineNumber, @ErrorLine, @ScriptLineNumber, SUSER_SNAME(), HOST_NAME(), @Note)
GO
